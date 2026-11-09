"""A model client, a fake one for tests and benchmarks, and a gateway in front of them.

The gateway is the only place the rest of the system talks to a model through.
It applies a rate limit, retries transient failures with backoff and jitter,
and stops calling a model that keeps failing (a circuit breaker).
"""
import hashlib
import random
import threading
import time
from typing import Callable, Protocol


class TransientLLMError(Exception):
    """A failure worth retrying: timeout, 429, 503."""


class CircuitOpen(Exception):
    """The model has been failing, so calls are being rejected for a short while."""


class LLM(Protocol):
    def complete(self, prompt: str) -> str: ...


class FakeLLM:
    """A model that answers deterministically, with adjustable latency and failures.

    The answer depends only on the prompt, which keeps benchmarks repeatable.
    Failures are random, because real ones are.
    """

    def __init__(self, latency: float = 0.0, failure_rate: float = 0.0, seed: int | None = None):
        self.latency, self.failure_rate = latency, failure_rate
        self._rng = random.Random(seed)
        self.calls = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        if self.latency:
            time.sleep(self.latency)
        if self.failure_rate and self._rng.random() < self.failure_rate:
            raise TransientLLMError("fake model unavailable")
        digest = hashlib.sha256(prompt.encode()).hexdigest()[:6]
        return f"send an email to user-{digest}"


class TokenBucket:
    """Allows `rate` calls per second on average, with bursts up to `burst`."""

    def __init__(self, rate: float, burst: int, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.rate, self.capacity = rate, float(burst)
        self._tokens, self._clock, self._sleep = float(burst), clock, sleep
        self._last = clock()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Take one token, waiting if none are available."""
        while True:
            with self._lock:
                now = self._clock()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self.rate
            self._sleep(wait)


class CircuitBreaker:
    """Opens after `threshold` failures in a row, and lets one call through after `reset` seconds."""

    def __init__(self, threshold: int = 5, reset: float = 2.0, clock: Callable[[], float] = time.monotonic):
        self.threshold, self.reset, self._clock = threshold, reset, clock
        self._failures = 0
        self._opened_at: float | None = None
        self._lock = threading.Lock()

    def before_call(self) -> None:
        with self._lock:
            if self._opened_at is None:
                return
            if self._clock() - self._opened_at >= self.reset:
                self._opened_at = None          # half open: let a trial call through
                self._failures = self.threshold - 1
                return
            raise CircuitOpen("too many recent failures")

    def record(self, ok: bool) -> None:
        with self._lock:
            if ok:
                self._failures, self._opened_at = 0, None
                return
            self._failures += 1
            if self._failures >= self.threshold:
                self._opened_at = self._clock()


def backoff_delay(attempt: int, base: float, cap: float, rng: Callable[[float, float], float] = random.uniform) -> float:
    """Full jitter: a random wait between 0 and min(cap, base * 2^attempt)."""
    return rng(0, min(cap, base * 2 ** attempt))


class LLMGateway:
    def __init__(self, llm: LLM, rate: float | None = None, burst: int = 10, retries: int = 3,
                 base_delay: float = 0.05, max_delay: float = 1.0, breaker: CircuitBreaker | None = None,
                 sleep: Callable[[float], None] = time.sleep):
        self.llm, self.retries = llm, retries
        self.base_delay, self.max_delay = base_delay, max_delay
        self.bucket = TokenBucket(rate, burst) if rate else None
        self.breaker = breaker or CircuitBreaker()
        self._sleep = sleep

    def complete(self, prompt: str) -> str:
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            self.breaker.before_call()
            if self.bucket:
                self.bucket.acquire()
            try:
                text = self.llm.complete(prompt)
            except TransientLLMError as exc:
                self.breaker.record(False)
                last = exc
                if attempt < self.retries:
                    self._sleep(backoff_delay(attempt, self.base_delay, self.max_delay))
                continue
            self.breaker.record(True)
            return text
        raise last

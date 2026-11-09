import pytest

from ratchet.llm import (CircuitBreaker, CircuitOpen, FakeLLM, LLMGateway, TokenBucket, TransientLLMError,
                         backoff_delay)


class FlakyLLM:
    """Fails the first `fail_times` calls, then answers."""

    def __init__(self, fail_times):
        self.fail_times, self.calls = fail_times, 0

    def complete(self, prompt):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise TransientLLMError("503")
        return "ok"


def test_fake_llm_answers_the_same_prompt_the_same_way():
    llm = FakeLLM()
    assert llm.complete("round 1") == llm.complete("round 1")
    assert llm.complete("round 1") != llm.complete("round 2")


def test_gateway_retries_transient_failures():
    llm = FlakyLLM(fail_times=2)
    gateway = LLMGateway(llm, retries=3, sleep=lambda s: None)
    assert gateway.complete("hi") == "ok"
    assert llm.calls == 3


def test_gateway_gives_up_after_its_retries():
    llm = FlakyLLM(fail_times=99)
    gateway = LLMGateway(llm, retries=2, breaker=CircuitBreaker(threshold=99), sleep=lambda s: None)
    with pytest.raises(TransientLLMError):
        gateway.complete("hi")
    assert llm.calls == 3   # the first try plus two retries


def test_backoff_is_jittered_and_capped():
    # With the random part pinned to its maximum we see the ceiling grow, then stop at the cap.
    ceilings = [backoff_delay(a, base=0.1, cap=1.0, rng=lambda lo, hi: hi) for a in range(6)]
    assert ceilings == [0.1, 0.2, 0.4, 0.8, 1.0, 1.0]
    # With real randomness it stays inside the range.
    assert all(0 <= backoff_delay(3, 0.1, 1.0) <= 0.8 for _ in range(200))


def test_token_bucket_limits_the_rate():
    now = [0.0]
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    bucket = TokenBucket(rate=2, burst=2, clock=lambda: now[0], sleep=sleep)
    for _ in range(2):
        bucket.acquire()            # the burst goes straight through
    assert slept == []
    bucket.acquire()                # the third has to wait for a token: 1 / 2 per second
    assert slept == [pytest.approx(0.5)]


def test_circuit_opens_after_repeated_failures_then_recovers():
    now = [0.0]
    breaker = CircuitBreaker(threshold=3, reset=10, clock=lambda: now[0])
    llm = FlakyLLM(fail_times=3)
    gateway = LLMGateway(llm, retries=0, breaker=breaker, sleep=lambda s: None)

    for _ in range(3):
        with pytest.raises(TransientLLMError):
            gateway.complete("hi")

    with pytest.raises(CircuitOpen):      # now open: the model isn't even called
        gateway.complete("hi")
    assert llm.calls == 3

    now[0] = 11                           # after the reset time one trial call is allowed
    assert gateway.complete("hi") == "ok"
    assert gateway.complete("hi") == "ok"

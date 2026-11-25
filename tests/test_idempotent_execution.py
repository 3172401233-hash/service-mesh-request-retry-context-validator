import unittest

from retryctx import (
    CONFLICT,
    IN_FLIGHT,
    REPLAY,
    IdempotencyStore,
    sha256_digest,
)


class PaymentProcessor:
    """Minimal side-effect owner backed by the digest-bound store."""

    def __init__(self):
        self.store = IdempotencyStore()
        self.charge_calls = 0

    def handle(self, key, digest):
        outcome = self.store.begin(key, digest)
        if outcome.outcome == REPLAY:
            return "replay", outcome.record.response_body
        if outcome.outcome == CONFLICT:
            return "conflict", None
        if outcome.outcome == IN_FLIGHT:
            return "in_flight", None
        self.charge_calls += 1
        response = {"charge_id": "ch-%d" % self.charge_calls}
        self.store.complete(key, digest, 200, response)
        return "executed", response


class IdempotentExecutionTests(unittest.TestCase):
    def test_retry_with_same_key_and_digest_executes_once(self):
        processor = PaymentProcessor()
        digest = sha256_digest(b"payment-body")

        first_state, first_body = processor.handle("idem-pay-1", digest)
        # Response is lost; caller retries with the SAME key and canonical digest.
        second_state, second_body = processor.handle("idem-pay-1", digest)

        self.assertEqual(first_state, "executed")
        self.assertEqual(second_state, REPLAY)
        self.assertEqual(processor.charge_calls, 1)
        self.assertEqual(first_body, second_body)

    def test_same_key_with_different_body_is_conflict_not_replay(self):
        processor = PaymentProcessor()
        digest_one = sha256_digest(b"99.00")
        digest_two = sha256_digest(b"9999.00")

        processor.handle("idem-pay-2", digest_one)
        state, body = processor.handle("idem-pay-2", digest_two)

        self.assertEqual(state, CONFLICT)
        self.assertIsNone(body)
        self.assertEqual(processor.charge_calls, 1)

    def test_concurrent_retry_sees_in_flight(self):
        store = IdempotencyStore()
        digest = sha256_digest(b"x")
        store.begin("k", digest)
        self.assertEqual(store.begin("k", digest).outcome, IN_FLIGHT)


if __name__ == "__main__":
    unittest.main()

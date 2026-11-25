import unittest

from retryctx import (
    CONFLICT,
    REPLAY,
    HttpRequest,
    IdempotencyStore,
    canonical_body_digest,
    sha256_digest,
    validate_scenario,
)


LOGICAL_BODY = '{"order_id":"O-1","amount":9900}'
OTHER_BODY = '{"order_id":"O-2","amount":1}'


class DecompressionDigestTests(unittest.TestCase):
    def setUp(self):
        self.request = HttpRequest.from_text(LOGICAL_BODY, compress="gzip")
        self.canonical = self.request.body_digest()
        self.wire = self.request.wire_digest()

    def test_wire_and_canonical_digests_differ_after_gzip(self):
        self.assertEqual(self.request.content_encoding, "gzip")
        self.assertNotEqual(self.wire, self.canonical)
        # Canonical digest is the plaintext logical body digest.
        self.assertEqual(
            self.canonical, sha256_digest(LOGICAL_BODY.encode("utf-8"))
        )
        # And the helper agrees even when handed compressed bytes.
        self.assertEqual(
            canonical_body_digest(self.request.body, "gzip"), self.canonical
        )

    def test_canonical_digest_stays_stable_through_first_hop(self):
        scenario = {
            "original_request": self.request,
            "idempotency_key": "idem-digest-1",
            "body_digest": self.canonical,
            "call_chain": [
                {
                    "name": "gateway->order",
                    "attempts": [
                        {
                            "attempt_id": "a1",
                            "response_status": 503,
                            "idempotency_key": "idem-digest-1",
                            "body_digest": self.canonical,
                        }
                    ],
                }
            ],
        }
        report = validate_scenario(scenario)
        digest_info = report["digest"]
        self.assertTrue(digest_info["decompression_applied"])
        self.assertTrue(digest_info["digest_changed_after_decompression"])
        self.assertTrue(digest_info["claimed_digest_matches"])
        codes = [f["code"] for f in report["findings"]]
        self.assertNotIn("BODY_DIGEST_MISMATCH", codes)
        self.assertEqual(report["positions"][0]["decision"], "retryable")

    def test_retry_with_changed_body_is_rejected(self):
        other_digest = sha256_digest(OTHER_BODY.encode("utf-8"))
        scenario = {
            "original_request": self.request,
            "idempotency_key": "idem-digest-2",
            "body_digest": self.canonical,
            "call_chain": [
                {
                    "name": "gateway->order",
                    "attempts": [
                        {
                            "attempt_id": "a1",
                            "response_status": 503,
                            "idempotency_key": "idem-digest-2",
                            "body_digest": other_digest,
                        }
                    ],
                }
            ],
        }
        report = validate_scenario(scenario)
        position = report["positions"][0]
        self.assertEqual(position["decision"], "blocked")
        self.assertIn("BODY_DIGEST_MISMATCH", position["blockers"])
        self.assertEqual(report["retryable_positions"], [])

    def test_binding_to_stale_wire_digest_is_flagged(self):
        scenario = {
            "original_request": self.request,
            "idempotency_key": "idem-digest-3",
            # Entry mistakenly binds the compressed wire digest.
            "body_digest": self.wire,
            "call_chain": [],
        }
        report = validate_scenario(scenario)
        self.assertTrue(report["digest"]["stale_wire_digest_binding"])
        codes = [f["code"] for f in report["findings"]]
        self.assertIn("STALE_WIRE_DIGEST_BINDING", codes)
        self.assertIn("DIGEST_CHANGED_AFTER_DECOMPRESSION", codes)


class IdempotencyStoreBindingTests(unittest.TestCase):
    def test_same_key_same_digest_replays_stored_result(self):
        store = IdempotencyStore()
        digest = sha256_digest(b"hello")
        first = store.begin("k-1", digest, attempt_id="a1")
        self.assertEqual(first.outcome, "new")
        store.complete("k-1", digest, 201, {"order_id": "O-1"})

        second = store.begin("k-1", digest, attempt_id="a1-retry1")
        self.assertEqual(second.outcome, REPLAY)
        self.assertTrue(second.replay_allowed)
        self.assertEqual(second.record.response_status, 201)
        self.assertEqual(second.record.response_body, {"order_id": "O-1"})

    def test_same_key_different_digest_conflicts_and_cannot_reuse_result(self):
        store = IdempotencyStore()
        digest_one = sha256_digest(b"body-one")
        digest_two = sha256_digest(b"body-two")
        store.begin("k-2", digest_one)
        store.complete("k-2", digest_one, 200, {"secret": "first-result"})

        clash = store.lookup("k-2", digest_two)
        self.assertEqual(clash.outcome, CONFLICT)
        self.assertFalse(clash.replay_allowed)
        # The stored result stays bound to the first digest only.
        self.assertEqual(clash.record.body_digest, digest_one)
        with self.assertRaises(ValueError):
            store.complete("k-2", digest_two, 200, {"secret": "second-result"})

    def test_concurrent_duplicate_is_in_flight(self):
        store = IdempotencyStore()
        digest = sha256_digest(b"ping")
        self.assertEqual(store.begin("k-3", digest).outcome, "new")
        duplicate = store.begin("k-3", digest)
        self.assertEqual(duplicate.outcome, "in_flight")


if __name__ == "__main__":
    unittest.main()

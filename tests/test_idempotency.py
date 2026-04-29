import json
import unittest

from app.services.idempotency import canonical_json, compute_idempotency_key


class IdempotencyTests(unittest.TestCase):
    def test_canonical_json_sorts_object_keys_recursively(self) -> None:
        left = {"b": 2, "a": {"d": 4, "c": 3}}
        right = {"a": {"c": 3, "d": 4}, "b": 2}

        self.assertEqual(canonical_json(left), canonical_json(right))
        self.assertEqual(
            compute_idempotency_key(left),
            compute_idempotency_key(right),
        )

    def test_canonical_json_accepts_jsonb_string_payloads(self) -> None:
        payload = json.dumps({"tracking": "1Z999", "carrier": "FastShip"})

        self.assertEqual(
            canonical_json(payload),
            '{"carrier":"FastShip","tracking":"1Z999"}',
        )

    def test_array_order_remains_significant(self) -> None:
        self.assertNotEqual(
            compute_idempotency_key({"items": [1, 2]}),
            compute_idempotency_key({"items": [2, 1]}),
        )


if __name__ == "__main__":
    unittest.main()


import unittest
from datetime import datetime, timedelta, timezone

from fraud_detector import FraudDetector


class FraudDetectorTests(unittest.TestCase):
    def make_transaction(self, number, customer="CLI-001",
                         amount=100.0, city="Crato", seconds=0):
        timestamp = datetime(
            2026, 10, 13, 12, 0, 0, tzinfo=timezone.utc
        ) + timedelta(seconds=seconds)

        return {
            "transaction_id": f"transacao-{number}",
            "customer_id": customer,
            "amount": amount,
            "city": city,
            "timestamp": timestamp.isoformat(),
        }

    def test_high_value(self):
        detector = FraudDetector()

        alerts = detector.detect(
            self.make_transaction(1, amount=10000)
        )

        self.assertIn("ALTO_VALOR", alerts)

    def test_four_transactions_under_60_seconds(self):
        detector = FraudDetector()

        for number in range(1, 4):
            alerts = detector.detect(
                self.make_transaction(number, seconds=number * 10)
            )

        alerts = detector.detect(
            self.make_transaction(4, seconds=40)
        )

        self.assertIn("TEMPO_60s", alerts)

    def test_different_cities(self):
        detector = FraudDetector()

        detector.detect(self.make_transaction(1, city="Crato"))

        alerts = detector.detect(
            self.make_transaction(2, city="Fortaleza", seconds=60)
        )

        self.assertIn("GEO_10m", alerts)


if __name__ == "__main__":
    unittest.main()

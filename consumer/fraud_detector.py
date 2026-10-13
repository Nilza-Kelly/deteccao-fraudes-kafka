
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone


HIGH_VALUE_LIMIT = 10000
TIME_WINDOW_SECONDS = 60
GEO_WINDOW_MINUTES = 10
MIN_TRANSACTIONS = 4


class FraudDetector:
    def __init__(self):
        self.customer_transactions = defaultdict(deque)

    def detect(self, transaction):
        customer_id = transaction["customer_id"]
        amount = float(transaction["amount"])
        city = transaction["city"]

        timestamp = datetime.fromisoformat(
            transaction["timestamp"].replace("Z", "+00:00")
        )

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        timestamp = timestamp.astimezone(timezone.utc)

        history = self.customer_transactions[customer_id]
        alerts = []

        # Mantém apenas transações relevantes para a janela de 10 minutos.
        geo_cutoff = timestamp - timedelta(minutes=GEO_WINDOW_MINUTES)

        while history and history[0]["timestamp"] < geo_cutoff:
            history.popleft()

        # Regra 1: valor elevado.
        if amount >= HIGH_VALUE_LIMIT:
            alerts.append("ALTO_VALOR")

        # Regra 2: quatro ou mais transações em menos de 60 segundos.
        recent_transactions = [
            item for item in history
            if timedelta(0) <= timestamp - item["timestamp"]
            < timedelta(seconds=TIME_WINDOW_SECONDS)
        ]

        if len(recent_transactions) + 1 >= MIN_TRANSACTIONS:
            alerts.append("TEMPO_60s")

        # Regra 3: cidades diferentes em uma janela de 10 minutos.
        recent_cities = {
            item["city"] for item in history
            if timedelta(0) <= timestamp - item["timestamp"]
            <= timedelta(minutes=GEO_WINDOW_MINUTES)
        }

        if any(previous_city != city for previous_city in recent_cities):
            alerts.append("GEO_10m")

        # Registra a transação atual para analisar as próximas.
        history.append({
            "timestamp": timestamp,
            "city": city,
        })

        return alerts

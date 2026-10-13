
import random
import uuid
from datetime import datetime, timezone


CUSTOMERS = [
    "CLI-001",
    "CLI-002",
    "CLI-003",
    "CLI-004",
    "CLI-005",
]

CITIES = [
    "Juazeiro do Norte",
    "Crato",
    "Barbalha",
    "Fortaleza",
    "Sobral",
]


def generate_transaction():
    """Gera uma transação simulada."""

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(50, 15000), 2),
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return transaction

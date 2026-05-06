import json
import random
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData

# Load secrets from .env file
load_dotenv()

# Initialize Faker
fake = Faker()

# Event Hub connection
CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# --- Transaction configuration ---
TRANSACTION_TYPES = ["purchase", "refund", "transfer", "withdrawal"]
CURRENCIES = ["USD", "EUR", "GBP", "INR", "AUD", "CAD"]
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant",
    "travel", "clothing", "entertainment",
    "healthcare", "fuel", "education"
]
HIGH_RISK_COUNTRIES = ["NG", "RU", "KP", "IR", "VE"]
ALL_COUNTRIES = ["US", "GB", "IN", "AU", "CA", "DE", "FR"] + HIGH_RISK_COUNTRIES

def is_fraud(amount, country, hour, user_id, transaction_count):
    """
    Rule-based fraud detection logic.
    Returns True if transaction looks suspicious.
    """
    score = 0

    # High amount
    if amount > 8000:
        score += 3

    # High risk country
    if country in HIGH_RISK_COUNTRIES:
        score += 2

    # Unusual hour (2am - 5am)
    if 2 <= hour <= 5:
        score += 1

    # High velocity - too many transactions
    if transaction_count > 5:
        score += 3

    return score >= 4

def generate_transaction(user_transaction_counts):
    """Generate a single fake financial transaction."""

    # Pick a random user (we have 100 fake users)
    user_id = f"user_{random.randint(1, 100)}"

    # Track how many transactions this user has made
    user_transaction_counts[user_id] = user_transaction_counts.get(user_id, 0) + 1
    transaction_count = user_transaction_counts[user_id]

    amount = round(random.uniform(1.0, 10000.0), 2)
    country = random.choice(ALL_COUNTRIES)
    now = datetime.now(timezone.utc)
    hour = now.hour

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "merchant_name": fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "country": country,
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "card_last4": str(random.randint(1000, 9999)),
        "timestamp": now.isoformat(),
        "hour_of_day": hour,
        "is_fraud": is_fraud(amount, country, hour, user_id, transaction_count),
        "transaction_count_per_user": transaction_count
    }

    return transaction

def send_to_eventhub(producer, transactions):
    """Send a batch of transactions to Event Hubs."""
    event_data_batch = producer.create_batch()

    for transaction in transactions:
        event_data_batch.add(EventData(json.dumps(transaction)))

    producer.send_batch(event_data_batch)

def main():
    print("🚀 Starting FinStream data generator...")
    print(f"📡 Connecting to Event Hub: {EVENT_HUB_NAME}")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    user_transaction_counts = {}
    total_sent = 0

    with producer:
        while True:
            # Generate a batch of 5 transactions every 2 seconds
            batch = []
            for _ in range(5):
                transaction = generate_transaction(user_transaction_counts)
                batch.append(transaction)

            # Send to Event Hubs
            send_to_eventhub(producer, batch)
            total_sent += len(batch)

            # Print sample to console so we can see it working
            sample = batch[0]
            fraud_flag = "🚨 FRAUD" if sample["is_fraud"] else "✅ OK"
            print(
                f"[{sample['timestamp']}] "
                f"{fraud_flag} | "
                f"User: {sample['user_id']} | "
                f"Amount: {sample['currency']} {sample['amount']} | "
                f"Country: {sample['country']} | "
                f"Total sent: {total_sent}"
            )

            # Wait 2 seconds before next batch
            time.sleep(2)

if __name__ == "__main__":
    main()
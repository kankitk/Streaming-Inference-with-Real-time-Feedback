import json
import random
import time
import logging
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "transactions"

# Configure logging for debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Producer with optimized settings
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Ensure message durability
    linger_ms=5,  # Reduce latency
    batch_size=16384,  # Optimize batch processing
    retries=5  # Retry on failures
)

# Generate random transactions
def generate_transaction():
    return {
        "transaction_id": random.randint(10000, 99999),
        "user_id": random.randint(1, 1000),
        "amount": round(random.uniform(10, 5000), 2),
        "location": random.choice(["USA", "India", "UK", "Germany", "Australia"]),
        "device": random.choice(["Mobile", "Desktop", "Tablet"]),
        "transaction_type": random.choice(["Purchase", "Withdrawal", "Transfer"]),
        "time": time.time()
    }

# Simulated transaction load pattern
def produce_transactions():
    while True:
        load_type = random.choice(["high", "medium", "low"])  # Simulate traffic pattern
        
        if load_type == "high":
            num_transactions = random.randint(50, 100)  # Burst traffic to trigger upscaling
            interval = 0.1  # Faster transaction rate
        elif load_type == "medium":
            num_transactions = random.randint(20, 50)  # Normal traffic
            interval = 0.3
        else:
            num_transactions = random.randint(5, 20)  # Low traffic, triggering downscaling
            interval = 0.6

        logging.info(f"🚀 Generating {num_transactions} transactions with interval {interval}s")

        for _ in range(num_transactions):
            transaction = generate_transaction()
            producer.send(TOPIC_NAME, transaction)
            logging.info(f"📤 Sent: {transaction}")
            time.sleep(interval)  # Control transaction rate

        sleep_time = random.randint(5, 15)  # Pause before next burst
        logging.info(f"⏳ Cooldown for {sleep_time}s before next batch...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        produce_transactions()
    except KeyboardInterrupt:
        logging.info("🛑 Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()

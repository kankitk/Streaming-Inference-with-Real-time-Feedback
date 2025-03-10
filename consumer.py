import json
import time
import joblib
import random
import logging
import subprocess
from kafka import KafkaConsumer, KafkaProducer

# Kafka Config
TRANSACTIONS_TOPIC = "transactions"
FRAUD_DECISION_TOPIC = "fraud_decisions"
KAFKA_BROKER = "localhost:9092"

# Load AI Model
MODEL_PATH = "fraud_detection_model.pkl"
model = joblib.load(MODEL_PATH)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Consumer & Producer
consumer = KafkaConsumer(
    TRANSACTIONS_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="fraud-detection-group",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def predict_fraud(transaction):
    """Runs fraud detection on a transaction"""
    try:
        features = [[transaction["amount"], transaction["user_id"]]]  # Use actual features
        prediction = model.predict(features)[0]
        return bool(prediction)
    except Exception as e:
        logging.error(f"Error in fraud detection: {e}")
        return False

def process_transactions():
    """Continuously listens for transactions and makes fraud decisions."""
    logging.info("📡 Listening for transactions...")

    for msg in consumer:
        transaction = msg.value
        logging.info(f"🔍 Processing Transaction: {transaction}")

        is_fraud = predict_fraud(transaction)
        decision = "fraud" if is_fraud else "legit"

        result = {
            "transaction_id": transaction["transaction_id"],
            "decision": decision,
            "is_fraud": is_fraud
        }

        logging.info(f"📤 Sending Fraud Decision: {result}")
        producer.send(FRAUD_DECISION_TOPIC, result)

        time.sleep(0.5)  # Control message processing speed

if __name__ == "__main__":
    try:
        process_transactions()
    except KeyboardInterrupt:
        logging.info("🛑 Consumer Stopped.")

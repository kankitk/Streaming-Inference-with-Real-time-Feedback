import json
import time
import logging
import pandas as pd
import joblib
from kafka import KafkaConsumer
import subprocess

# Kafka Config
FEEDBACK_TOPIC = "fraud_feedback"
TRANSACTIONS_CSV = "transactions.csv"
MODEL_PATH = "fraud_detection_model.pkl"
KAFKA_BROKER = "localhost:9092"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Consumer for feedback
consumer = KafkaConsumer(
    FEEDBACK_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="feedback-group",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def update_transactions_csv(transaction_id, correct_label):
    """Updates transactions.csv with the corrected fraud label"""
    try:
        df = pd.read_csv(TRANSACTIONS_CSV)
        df.loc[df["transaction_id"] == transaction_id, "fraud"] = correct_label
        df.to_csv(TRANSACTIONS_CSV, index=False)
        logging.info(f"✅ Updated transaction {transaction_id} with correct label: {correct_label}")
    except Exception as e:
        logging.error(f"⚠️ Error updating transactions.csv: {e}")

def handle_feedback():
    """Listens for fraud feedback and updates model training data"""
    logging.info("📡 Listening for fraud feedback...")

    for msg in consumer:
        feedback = msg.value
        logging.info(f"🔄 Received Feedback: {feedback}")

        transaction_id = feedback["transaction_id"]
        correct_label = int(feedback["correct_fraud_label"])  # 0 = Legit, 1 = Fraud

        update_transactions_csv(transaction_id, correct_label)

        # Retrain model periodically (every 10 updates)
        if feedback["retrain"]:
            logging.info("🆕 Retraining model with new feedback data...")
            subprocess.run(["python3", "train_model.py"])

if __name__ == "__main__":
    try:
        handle_feedback()
    except KeyboardInterrupt:
        logging.info("🛑 Feedback Handler Stopped.")

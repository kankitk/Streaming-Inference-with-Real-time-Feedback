import pandas as pd
import numpy as np
import joblib
import json
import logging
import time
from kafka import KafkaConsumer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
MODEL_PATH = "fraud_detection_model.pkl"
DATA_PATH = "transactions.csv"
TOPIC_NAME = "fraud_feedback"  # Topic for real-time feedback
KAFKA_BROKER = "localhost:9092"
TRAINING_INTERVAL = 60  # Retrain every 60 seconds

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

def load_data():
    """Loads transaction data for training"""
    try:
        return pd.read_csv(DATA_PATH)
    except FileNotFoundError:
        logging.warning("⚠️ No existing transaction data found. Creating new dataset.")
        return pd.DataFrame(columns=["user_id", "amount", "fraud"])

def preprocess_data(df):
    """Prepares data for training."""
    if df.empty:
        return None, None

    df.fillna(0, inplace=True)  # Handle missing values
    X = df[["amount", "user_id"]]  # Select features
    y = df["fraud"]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, y

def train_model():
    """Trains the fraud detection AI model."""
    logging.info("🔄 Retraining AI fraud detection model...")
    df = load_data()
    X, y = preprocess_data(df)

    if X is None:
        logging.warning("❌ No data available for training.")
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    joblib.dump(model, MODEL_PATH)
    logging.info("✅ Model retrained and saved.")

def continuous_training():
    """Continuously updates dataset and retrains model based on real-time feedback."""
    logging.info("📡 Listening for real-time fraud feedback...")

    while True:
        new_data = []
        
        for message in consumer:
            feedback = message.value
            logging.info(f"📥 Received Feedback: {feedback}")

            new_data.append(feedback)
            
            # Stop collecting after an interval
            if len(new_data) >= 50:  
                break
        
        if new_data:
            df_new = pd.DataFrame(new_data)
            df_existing = load_data()
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_csv(DATA_PATH, index=False)
            logging.info("✅ New feedback data saved.")

            # Retrain model
            train_model()

        time.sleep(TRAINING_INTERVAL)

if __name__ == "__main__":
    continuous_training()

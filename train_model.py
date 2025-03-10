import pandas as pd
import numpy as np
import joblib
import logging
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MODEL_PATH = "fraud_detection_model.pkl"
DATA_PATH = "transactions.csv"

def load_data():
    """Loads transaction data for training"""
    try:
        df = pd.read_csv(DATA_PATH)
        if df.empty:
            raise FileNotFoundError  # Treat empty files as missing
        return df
    except (FileNotFoundError, pd.errors.EmptyDataError):
        logging.warning("⚠️ No existing transaction data found. Creating sample dataset.")
        return create_sample_data()

def create_sample_data():
    """Creates a small sample dataset for initial training."""
    df = pd.DataFrame({
        "user_id": [101, 102, 103, 104, 105],
        "amount": [500, 1500, 700, 3000, 200],
        "fraud": [0, 1, 0, 1, 0]
    })
    df.to_csv(DATA_PATH, index=False)
    return df

def preprocess_data(df):
    """Prepares data for training."""
    df.fillna(0, inplace=True)  # Handle missing values
    X = df[["amount", "user_id"]]  # Select features
    y = df["fraud"]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, y

def train_model():
    """Trains the fraud detection AI model."""
    logging.info("🔄 Training AI fraud detection model...")
    df = load_data()
    X, y = preprocess_data(df)

    if X is None:
        logging.warning("❌ No data available for training.")
        return

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    joblib.dump(model, MODEL_PATH)
    logging.info("✅ Model trained and saved.")

if __name__ == "__main__":
    train_model()

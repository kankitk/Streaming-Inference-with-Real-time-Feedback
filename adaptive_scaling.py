import subprocess
import time
from kafka import KafkaAdminClient, KafkaConsumer
import logging
import numpy as np
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import psutil  # For fetching system resource usage like CPU and memory

# Constants
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "transactions"
MAX_CONSUMERS = 50
MIN_CONSUMERS = 1
MODEL_PATH = "path_to_trained_model.pkl"  # Path to the saved model
TRAINING_DATA_BUFFER_SIZE = 1000  # Number of data points to keep for retraining

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load the trained model if exists, else initialize a new one
try:
    with open(MODEL_PATH, 'rb') as model_file:
        model = pickle.load(model_file)
except FileNotFoundError:
    logging.info("No pre-trained model found, initializing a new model.")
    model = RandomForestRegressor(n_estimators=100)

# Data buffer for training
training_data = []

def get_kafka_lag():
    """Fetch Kafka consumer lag by comparing partition offsets."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            group_id="fraud-detection-group",
            enable_auto_commit=False
        )

        end_offsets = admin_client.list_consumer_group_offsets("fraud-detection-group")
        total_lag = sum(
            max(consumer.end_offsets([partition])[partition] - (meta.offset or 0), 0)
            for partition, meta in end_offsets.items()
        )

        return total_lag
    except Exception as e:
        logging.error(f"⚠️ Error fetching Kafka lag: {e}")
        return 0

def get_system_metrics():
    """Fetch system metrics like CPU, memory, and active requests."""
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in percentage
    memory_usage = psutil.virtual_memory().percent  # Memory usage in percentage
    active_requests = 10  # Replace with actual active request fetching logic if possible

    return cpu_usage, memory_usage, active_requests

def update_training_data(lag, cpu_usage, memory_usage, active_requests, active_consumers):
    """Update the training data buffer with new metrics."""
    global training_data
    data_point = [lag, cpu_usage, memory_usage, active_requests, active_consumers]
    training_data.append(data_point)

    # Ensure we don't exceed the buffer size
    if len(training_data) > TRAINING_DATA_BUFFER_SIZE:
        training_data.pop(0)

def train_model():
    """Train or retrain the model with the latest data."""
    if len(training_data) < 2:
        logging.info("Not enough data to train the model.")
        return

    # Split the data into features and labels
    data = np.array(training_data)
    X = data[:, :-1]  # Features: lag, cpu_usage, memory_usage, active_requests
    y = data[:, -1]   # Labels: active_consumers

    # Train the model
    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)
    model.fit(X_train, y_train)

    # Save the trained model
    with open(MODEL_PATH, 'wb') as model_file:
        pickle.dump(model, model_file)

    logging.info("Model retrained and saved.")

def predict_consumers(lag, cpu_usage, memory_usage, active_requests):
    """Predict the optimal number of consumers using the trained model."""
    features = np.array([[lag, cpu_usage, memory_usage, active_requests]])

    # Check if the model is fitted before making a prediction
    try:
        predicted_consumers = model.predict(features)
        return int(predicted_consumers[0])  # Convert to integer
    except Exception as e:
        logging.error(f"⚠️ Model not fitted yet: {e}")
        return MIN_CONSUMERS  # Default to minimum consumers if model isn't trained

def spawn_consumer():
    """Spawns a new consumer process."""
    try:
        logging.info("⚡ Spawning additional consumer...")
        subprocess.Popen(["python3", "consumer.py"])
    except Exception as e:
        logging.error(f"⚠️ Error spawning consumer: {e}")

def stop_consumer():
    """Stops a consumer process."""
    try:
        logging.info("✅ Load stabilized! Stopping a consumer...")
        subprocess.run(["pkill", "-f", "consumer.py"])
    except Exception as e:
        logging.error(f"⚠️ Error stopping consumer: {e}")

def scale_consumers():
    """Dynamically adjust consumer instances based on AI/ML model predictions."""
    active_consumers = MIN_CONSUMERS

    while True:
        # Get the current system metrics
        lag = get_kafka_lag()
        cpu_usage, memory_usage, active_requests = get_system_metrics()

        logging.info(f"📊 Current Kafka Lag: {lag}, Active Consumers: {active_consumers}, CPU: {cpu_usage}%, Memory: {memory_usage}%")

        # Update training data buffer with the latest metrics
        update_training_data(lag, cpu_usage, memory_usage, active_requests, active_consumers)

        # Retrain the model periodically (every 100 iterations or after enough data is collected)
        # if len(training_data) % 10 == 0:
        train_model()

        # Predict the optimal number of consumers
        predicted_consumers = predict_consumers(lag, cpu_usage, memory_usage, active_requests)
        logging.info(f"Predicted number of consumers: {predicted_consumers}")

        # Scale up or down based on the predicted number of consumers
        if predicted_consumers > active_consumers and active_consumers < MAX_CONSUMERS:
            while active_consumers < predicted_consumers:
                spawn_consumer()
                active_consumers += 1

        elif predicted_consumers < active_consumers and active_consumers > MIN_CONSUMERS:
            while active_consumers > predicted_consumers:
                stop_consumer()
                active_consumers -= 1

        time.sleep(5)  # Adjust based on your preference for scaling frequency

if __name__ == "__main__":
    scale_consumers()

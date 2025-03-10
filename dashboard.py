import json
import logging
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer

# Flask and SocketIO setup
app = Flask(__name__)
socketio = SocketIO(app)

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
FRAUD_DECISION_TOPIC = "fraud_decisions"

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Consumer to listen for fraud decision updates
consumer = KafkaConsumer(
    FRAUD_DECISION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="fraud-dashboard-group",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# List to hold the recent fraud decision messages
fraud_decisions = []

def listen_for_fraud_decisions():
    """Listen for fraud decision messages from Kafka and send them to the dashboard via SocketIO."""
    for message in consumer:
        fraud_decision = message.value
        fraud_decisions.append(fraud_decision)

        # Keep only the latest 10 fraud decisions for the dashboard
        if len(fraud_decisions) > 10:
            fraud_decisions.pop(0)

        # Emit fraud decision updates to the frontend dashboard
        socketio.emit('new_fraud_decision', fraud_decision)

@app.route('/')
def index():
    """Render the dashboard HTML."""
    return render_template('index.html', fraud_decisions=fraud_decisions)

if __name__ == "__main__":
    # Start listening to Kafka in the background
    socketio.start_background_task(listen_for_fraud_decisions)
    
    # Run Flask app
    socketio.run(app, host='0.0.0.0', port=5001)

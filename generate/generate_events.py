from datetime import datetime
import time
import json
import random
import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.cloud import storage
import paho.mqtt.client as mqtt
import concurrent.futures

# Load environment variables from .env file
load_dotenv()

PROJECT_ID = os.environ['PROJECT_ID']
PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']
BUCKET_NAME=os.environ['BUCKET_NAME']

# MQTT Broker Configuration (No Auth)
MQTT_BROKER = os.environ['MQTT_BROKER']
MQTT_PORT = int(os.environ['MQTT_PORT'])
MQTT_TOPIC = os.environ['MQTT_TOPIC']

# Initialize GCS Client
storage_client = storage.Client()
bucket = storage_client.get_bucket(BUCKET_NAME)

# Load songs from gs://BUCKET_NAME/songs_data.json
songs_blob = bucket.blob("songs_data.json")
songs = json.loads(songs_blob.download_as_string())

# Load all users from gs://BUCKET_NAME/users_data_*.json
users = []
blobs = bucket.list_blobs(prefix="users_data", delimiter=None)
for blob in blobs:
    if blob.name.endswith(".json") and "users_data" in blob.name and "schema" not in blob.name:
        data = json.loads(blob.download_as_string())
        users.extend(data)  # Assuming each file contains a list of user records
        print(f"ℹ️ Loaded {blob.name}")

if not users:
    raise ValueError("No matching users_data_*.json files found in gs://BUCKET_NAME")

# Loads possible events
events = ['login', 'play', 'pause', 'next', 'previous']

# Define events volume
events_per_second = 10
interval = 1.0 / events_per_second

# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

# Initialize MQTT client
client = mqtt.Client()
# Connect to the MQTT broker
client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_start()

def create_surrogate_key(event, user, timestamp):
    surrogate_key = f'{event}.{user}.{timestamp}'
    surrogate_key = hash(surrogate_key)
    return surrogate_key

def generate_event(songs, users):
    # Generate a random event from lists: users, songs, events
    user = random.choice(users)
    song = random.choice(songs)
    event = random.choice(events)
    timestamp = str(datetime.now())
    
    event = {
        'event_id': create_surrogate_key(event, user, timestamp),
        'event_type': event,
        'user_id': user['email'],
        'title': song['title'],
        'artist': song['artist'],
        'album': song['album'],
        'datetime': timestamp,
        'coordinates': user['location']['coordinates']
    }
    return event

def publish_event_pubsub(event):
    try:
        # Publishes a JSON message to Pub/Sub
        message_json = json.dumps(event)
        message_bytes = message_json.encode("utf-8")
        
        publisher.publish(topic_path, message_bytes)
    except Exception as e:
        print(f'Failed to publish message: {e}')

def publish_event_mqtt(event):
    try:
        # Publishes a JSON message to MQTT
        message_json = json.dumps(event)
        client.publish(MQTT_TOPIC, message_json)
    except Exception as e:
        print(f'Failed to publish message: {e}')

def worker():
    while True:
        event = generate_event(songs, users)
        publish_event_pubsub(event)
        publish_event_mqtt(event)
        # Random sleep between 50% and 150% of the base interval
        random_interval = interval * random.uniform(0.2, 1.5)
        time.sleep(random_interval)

# Use ThreadPoolExecutor to run two workers
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    print('Running on 2 workers...')
    # Submit the worker function twice
    executor.submit(worker)
    executor.submit(worker)
    # The 'with' block keeps running until interrupted

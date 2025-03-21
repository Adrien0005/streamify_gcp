from datetime import datetime
from zoneinfo import ZoneInfo
import time
import json
import random
import math
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

# Define event rate range (events per second)
LOW_RATE = 50    # Minimum during quiet periods
HIGH_RATE = 1000 # Maximum during busy periods
PERIOD = 600     # Seconds for one full cycle (10 minutes)

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
    timestamp = str(datetime.now(ZoneInfo('America/New_York')))[:26]
    
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
        print('Published message to Pub/Sub')
    except Exception as e:
        print(f'Failed to publish message: {e}')

def publish_event_mqtt(event):
    try:
        # Publishes a JSON message to MQTT
        message_json = json.dumps(event)
        client.publish(MQTT_TOPIC, message_json)
        print('Published message to MQTT')
    except Exception as e:
        print(f'Failed to publish message: {e}')

def worker():
    while True:
        # Define the random interval 
        try:
            elapsed = time.time() - start_time
            rate = LOW_RATE + (HIGH_RATE - LOW_RATE) * (0.5 + 0.5 * math.sin(2 * math.pi * elapsed / PERIOD))
            interval = 1.0 / rate
            random_interval = interval * random.uniform(0.8, 1.2)
            
            event = generate_event(songs, users)
            publish_event_pubsub(event)
            publish_event_mqtt(event)
            time.sleep(random_interval)
        except Exception as e:
            print(e)

# Use ThreadPoolExecutor to run two workers
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    start_time = time.time()
    
    print('Running on 2 workers...')
    # Submit the worker function twice
    executor.submit(worker)
    executor.submit(worker)
    # The 'with' block keeps running until interrupted

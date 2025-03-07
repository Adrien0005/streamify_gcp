from datetime import datetime
import time
import json
import random
import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
import paho.mqtt.client as mqtt


# Load environment variables from .env file
load_dotenv()

PROJECT_ID = os.environ['PROJECT_ID']
TOPIC_ID =  os.environ['TOPIC_ID']

# MQTT Broker Configuration (No Auth)
MQTT_BROKER = os.environ['MQTT_BROKER']
MQTT_PORT = int(os.environ['MQTT_PORT'])
MQTT_TOPIC = os.environ['MQTT_TOPIC']



with open('data/songs_data.json', 'r') as file:
    songs = json.load(file)
    
with open('data/users_data.json', 'r') as file:
    users = json.load(file)
    
events = ['play' ,'pause', 'next', 'previous']

events_per_second = 0.5
interval = 1.0 / events_per_second



# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Initialize MQTT client
client = mqtt.Client()
# Connect to the MQTT broker
client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_start()



def create_surrogate_key(event,user,datetime):
    surrogate_key = hash(event + user['email'] + str(datetime))
    return surrogate_key



def generate_event(songs, users):
    # Generate a random event from lists: users, songs, events
    user = random.choice(users)
    song = random.choice(songs)
    event = random.choice(events)
    
    event = {
            'event_id': create_surrogate_key(event,user,datetime),
            'event_type':event,
            'user_id':user['email'],
            'title':song['title'],
            'artist':song['artist'],
            'album':song['album'],
            'datetime': str(datetime.now()),
            'coordinates':user['location']['coordinates']
            }
    return event



def publish_event_pubsub(event):
    try:
        # Publishes a JSON message to Pub/Sub
        message_json = json.dumps(event)
        message_bytes = message_json.encode("utf-8")
        
        publisher.publish(topic_path, message_bytes)
        print('Published to Pub/Sub')

    except Exception as e:
        print(f'Failed to publish message: {e}')



def publish_event_mqtt(event):
    try:
        # Publishes a JSON message to MQTT
        message_json = json.dumps(event)
        client.publish(MQTT_TOPIC, message_json)
        print('Published to MQTT')
        
    except Exception as e:
        print(f'Failed to publish message: {e}')



while True:
    event = generate_event(songs, users)
    publish_event_pubsub(event)
    publish_event_mqtt(event)
    
    time.sleep(interval)

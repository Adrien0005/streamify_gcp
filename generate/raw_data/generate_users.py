import requests
import json
from google.cloud import storage  # Added for GCS
from google.cloud import bigquery
import os
from dotenv import load_dotenv
import datetime

# Load Environment Variables
load_dotenv()

# Initialize BigQuery Client
client = bigquery.Client()

# Define Constants
RAW_USERS_TABLE = os.environ['RAW_USERS_TABLE_ID']
PROJECT_ID = os.environ['PROJECT_ID']
RAW_USERS_TABLE_ID = f'{PROJECT_ID}.{RAW_USERS_TABLE}'

NUM_OF_USERS = 4000
URL = f'https://randomuser.me/api/?results={NUM_OF_USERS}'

def get_user_data(url):
    request = requests.get(url)
    data = request.json()
    return data['results']

def save_user_data(data):
    try:
        # Initialize GCS client and bucket
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("streamify_gcp")
        
        # Generate filename with datetime suffix
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"users_data_{timestamp}.json"
        
        # Upload directly to GCS without saving locally
        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
        
        print(f'✅ User Data uploaded to gs://streamify_gcp/{filename}')
    except Exception as e:
        print(f'❌ Error: {e}')

def main(url):
    data = get_user_data(url)
    save_user_data(data)

main(URL)

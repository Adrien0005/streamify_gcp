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
DIM_RAW_USERS_TABLE = os.environ['DIM_RAW_USERS_TABLE']
PROJECT_ID = os.environ['PROJECT_ID']
RAW_USERS_TABLE_ID = f'{PROJECT_ID}.{DIM_RAW_USERS_TABLE}'
BUCKET_NAME=os.environ['BUCKET_NAME']

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
        bucket = storage_client.get_bucket(BUCKET_NAME)
        
        # Generate filename with datetime suffix
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"users_data_{timestamp}.json"
        
        # Upload directly to GCS without saving locally
        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
        
        print(f'✅ User Data uploaded to gs://{BUCKET_NAME}/{filename}')
    except Exception as e:
        print(f'❌ Error: {e}')

def main(url):
    data = get_user_data(url)
    save_user_data(data)

main(URL)

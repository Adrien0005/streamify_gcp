import requests
import json
from google.cloud import bigquery  # type: ignore
import os
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

# Initialize Bigquery Client
client = bigquery.Client()

# Define Constant
RAW_USERS_TABLE= os.environ['RAW_USERS_TABLE_ID']
PROJECT_ID= os.environ['PROJECT_ID']
RAW_USERS_TABLE_ID = f'{PROJECT_ID}.{RAW_USERS_TABLE}'


NUM_OF_USERS=4000
URL=f'https://randomuser.me/api/?results={NUM_OF_USERS}'


def get_user_data(url):
    request = requests.get(url)
    data = request.json()
    return data['results']


def save_user_data(data):
    try:
        data = get_user_data(URL)
        # Save data as .json
        with open('extract/raw_data/users_data.json', 'w') as file:
            json.dump(data, file, indent= 4)
        print('✅ User Data saved to .json file')
    except Exception as e:
        print(f'❌ Error: {e}')
    return

        
def main(url):
    data = get_user_data(url)
    save_user_data(data)
    
main(URL)
    

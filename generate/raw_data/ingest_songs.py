import json
import pandas as pd
import os
import pandas_gbq
from dotenv import load_dotenv
from google.cloud import storage

# Load Environment Variables
load_dotenv()

DIM_RAW_SONGS_TABLE = os.environ['DIM_RAW_SONGS_TABLE']
PROJECT_ID = os.environ['PROJECT_ID']

# Initialize GCS client and get the file from the bucket
storage_client = storage.Client()
bucket = storage_client.get_bucket("streamify_gcp")
blob = bucket.blob("songs_data.json")

# Download the file content as a string and load it as JSON
data = json.loads(blob.download_as_string())

# Create a BigQuery table with songs library
df = pd.DataFrame(data)
pandas_gbq.to_gbq(df, DIM_RAW_SONGS_TABLE, PROJECT_ID, if_exists='append')

print(f'✅ Songs data pushed to BigQuery table {DIM_RAW_SONGS_TABLE}')

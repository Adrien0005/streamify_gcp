import json
from google.cloud import bigquery
from google.cloud import storage
import os
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

# Initialize BigQuery Client
client = bigquery.Client()

# Initialize GCS Client
storage_client = storage.Client()

RAW_USERS_TABLE = os.environ['RAW_USERS_TABLE_ID']
PROJECT_ID = os.environ['PROJECT_ID']
RAW_USERS_TABLE_ID = f'{PROJECT_ID}.{RAW_USERS_TABLE}'

def read_user_data():
    # Get all files matching 'users_data_*.json' from the bucket
    bucket = storage_client.get_bucket("streamify_gcp")
    blobs = bucket.list_blobs(prefix="users_", delimiter=None)
    
    # Combine data from all matching files
    all_data = []
    for blob in blobs:
        if blob.name.endswith(".json") and "users_data" in blob.name and "schema" not in blob.name:
            data = json.loads(blob.download_as_string())
            all_data.extend(data)  # Assuming each file contains a list of records
            print(f"ℹ️ Loaded {blob.name}")
    
    if not all_data:
        raise ValueError("No matching users_data_*.json files found in gs://streamify_gcp")
    return all_data

def create_bigquery_table(RAW_USERS_TABLE):
    # Read schema from GCS
    bucket = storage_client.get_bucket("streamify_gcp")
    schema_blob = bucket.blob("users_data_schema.json")
    schema = json.loads(schema_blob.download_as_string())
    
    try:
        # Check if the table already exists
        client.get_table(RAW_USERS_TABLE)  # This will raise NotFound if table doesn't exist
        print(f"ℹ️ Table {RAW_USERS_TABLE} already exists, skipping creation")
    except Exception as e:
        if "NotFound" in str(e):  # Table doesn’t exist, create it
            # Create table object with schema
            table = bigquery.Table(RAW_USERS_TABLE, schema=schema)
            table = client.create_table(table)
            print(f"✅ Table {RAW_USERS_TABLE} created successfully")
        else:
            print(f"❌ Encountered errors: {e}")

def ingest_to_bq(data):
    # Load to BigQuery
    try:
        errors = client.insert_rows_json(RAW_USERS_TABLE_ID, data)
        if errors == []:
            print('✅ Users data ingested successfully')
        else:
            print(f"❌ Encountered errors: {errors}")
    except Exception as e:
        print(f"❌ Encountered errors: {e}")

def main():
    data = read_user_data()
    create_bigquery_table(RAW_USERS_TABLE_ID)
    ingest_to_bq(data)

main()

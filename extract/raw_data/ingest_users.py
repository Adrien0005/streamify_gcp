import json
from google.cloud import bigquery  # type: ignore
import os
from dotenv import load_dotenv

# Load Environments Variables
load_dotenv()

# Initialize Bigquery Client
client = bigquery.Client()

RAW_USERS_TABLE= os.environ['RAW_USERS_TABLE_ID']
PROJECT_ID= os.environ['PROJECT_ID']
RAW_USERS_TABLE_ID = f'{PROJECT_ID}.{RAW_USERS_TABLE}'
FILE_PATH='extract/raw_data/users_data.json'



def read_user_data(FILE_PATH):
    with open(FILE_PATH, 'r') as user_data:
        data = json.load(user_data)
    return data


def create_bigquery_table(RAW_USERS_TABLE):
    with open('extract/raw_data/users_data_schema.json', 'r') as schema_file:
        schema = json.load(schema_file)
        try:
            # Create table object with schema
            table = bigquery.Table(RAW_USERS_TABLE, schema=schema)
            
            # Create or replace the table
            table = client.create_table(table, exists_ok=True)  # exists_ok=True will replace if exists
            print(f"✅ Table {RAW_USERS_TABLE} created or replaced successfully")
        except Exception as e:
            print(f"❌ Encountered errors: {e}")
            
    
def ingest_to_bq(data):
    # Load to Bigquery
    try:
        errors = client.insert_rows_json(RAW_USERS_TABLE_ID, data)
        if errors == []:
            print('✅ Users data ingested succesfully')
        else:
            print(f"❌ Encountered errors: {errors}")
    except Exception as e:
        print(f"❌ Encountered errors: {e}")
        
def main(FILE_PATH):
    data = read_user_data(FILE_PATH)
    create_bigquery_table(RAW_USERS_TABLE_ID)
    ingest_to_bq(data)
    
main(FILE_PATH)

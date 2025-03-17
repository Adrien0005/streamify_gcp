import base64
import functions_framework # type: ignore
from google.cloud import bigquery  # type: ignore
import json
import os

#Initialize Bigquery Client
client = bigquery.Client()

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def ingest_to_bq(event):
    #Environments parameters
    PROJECT_ID = os.environ['PROJECT_ID']
    FCT_RAW_CLOUD_FUNCT_EVENTS_TABLE= os.environ['FCT_RAW_CLOUD_FUNCT_EVENTS_TABLE']
    table_id = f'{PROJECT_ID}.{FCT_RAW_CLOUD_FUNCT_EVENTS_TABLE}'
    
    # Ingest json Pub/Sub event to Bigquery
    try:
        payload = base64.b64decode(event.data["message"]["data"])
        json_payload = json.loads(payload)
        errors = client.insert_rows_json(table_id, [json_payload])
        if errors == []:
            print('✅ event ingested succesfully')
        else:
            print(f"❌ Encountered errors: {errors}")
    except Exception as e:
        print(f"❌ Encountered errors: {e}")

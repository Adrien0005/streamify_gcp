#!/bin/bash

# Load environment variables from .env file
source .env

# Change folder
cd _init/

# 1. Create a bucket
gcloud storage buckets create gs://${BUCKET_NAME} \
  --project=${PROJECT_ID} \
  --location=${REGION}

# 2. Create BigQuery dataset and tables
# Create dataset
bq mk -d \
  --project_id=${PROJECT_ID} \
  ${PROJECT_ID}:${RAW_DATASET}

# Create tables with schemas
bq mk \
  --table \
  --project_id=${PROJECT_ID} \
  ${FCT_RAW_DATAFLOW_EVENTS_TABLE} \
  fct_events_table_schema.json

bq mk \
  --table \
  --project_id=${PROJECT_ID} \
  ${FCT_RAW_CLOUD_FUNCT_EVENTS_TABLE} \
  fct_events_table_schema.json

bq mk \
  --table \
  --project_id=${PROJECT_ID} \
  ${DIM_RAW_SONGS_TABLE} \
  dim_raw_songs_schema.json

bq mk \
  --table \
  --project_id=${PROJECT_ID} \
  ${DIM_RAW_USERS_TABLE} \
  dim_raw_users_table_schema.json

# 3. Load songs_data.json into bucket
gcloud storage cp songs_data.json gs://${BUCKET_NAME}/ \
  --project=${PROJECT_ID}

# 4. Create Pub/Sub topic
gcloud pubsub topics create ${PUBSUB_TOPIC} \
  --project=${PROJECT_ID}

# 5. Setup Pub/Sub schema
gcloud pubsub schemas create streamify-schema \
  --type=avro \
  --definition-file=streamify_pubsub_schema.json \
  --project=${PROJECT_ID}

gcloud pubsub topics update ${PUBSUB_TOPIC} \
  --message-encoding=JSON \
  --schema=projects/${PROJECT_ID}/schemas/streamify-schema \
  --project=${PROJECT_ID}

# 6. Setup Cloud Functions for ingestion
gcloud functions deploy ingest-from-pubsub \
  --runtime=python39 \
  --trigger-topic=${PUBSUB_TOPIC} \
  --entry-point=ingest_to_bq \
  --source=./cloud_function/ \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},PUBSUB_TOPIC=${PUBSUB_TOPIC}"

# 7. Setup Dataflow ingestion from Pub/Sub to BigQuery
gcloud dataflow jobs run pubsub-to-bigquery \
  --gcs-location=gs://dataflow-templates-${REGION}/latest/PubSub_to_BigQuery \
  --project=${PROJECT_ID} \
  --region=${REGION} \
  --parameters="inputTopic=projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC},outputTableSpec=${PROJECT_ID}:${RAW_DATASET}.dataflow_music_events"

# 8. Build and push container to Google Container Registry
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${CLOUD_RUN_NAME}:latest

# 9. Deploy Cloud Run job
gcloud run jobs create ${CLOUD_RUN_NAME} \
  --image gcr.io/${PROJECT_ID}/${CLOUD_RUN_NAME}:latest \
  --region ${REGION} \
  --project=${PROJECT_ID}

# 10. Schedule the dbt run
gcloud scheduler jobs create http dbt-scheduler-job \
  --schedule "0 * * * *" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${CLOUD_RUN_NAME}:run" \
  --http-method POST \
  --oauth-service-account-email cloud-scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
  --location ${REGION}

echo "✅ Setup complete! For the event generator, you'll need to run generate_events.py separately."

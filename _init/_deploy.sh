# setup pubsub

# setup schema pubsub

# setup cloud functions

# setup dataflow

# Build and push the container to Google Container Registry
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${CLOUD_RUN_NAME}:latest

# Deploy a Cloud Run job
gcloud run jobs create ${CLOUD_RUN_NAME} \
  --image gcr.io/${PROJECT_ID}/${CLOUD_RUN_NAME}:latest \
  --region ${REGION}

# schedule the dbt run
gcloud scheduler jobs create http dbt-scheduler-job \
  --schedule "0 * * * *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/dt-dta-adrien-sandbox-dev/jobs/dbt-streamify:run" \
  --http-method POST \
  --oauth-service-account-email cloud-scheduler@dt-dta-adrien-sandbox-dev.iam.gserviceaccount.com \
  --location us-central1

# launch event generator

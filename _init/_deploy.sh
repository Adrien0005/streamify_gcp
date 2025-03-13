# setup pubsub

# setup schema pubsub

# setup cloud functions

# setup dataflow

# build & push docker image
gcloud builds submit --tag gcr.io/dt-dta-adrien-sandbox-dev/dbt-streamify:latest

# create cloud run based on latest docker image
gcloud run jobs create dbt-streamify \
  --image gcr.io/dt-dta-adrien-sandbox-dev/dbt-streamify:latest \
  --region us-central1 

# schedule the dbt run
gcloud scheduler jobs create http dbt-scheduler-job \
  --schedule "0 * * * *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/dt-dta-adrien-sandbox-dev/jobs/dbt-streamify:run" \
  --http-method POST \
  --oauth-service-account-email cloud-scheduler@dt-dta-adrien-sandbox-dev.iam.gserviceaccount.com \
  --location us-central1

# launch event generator

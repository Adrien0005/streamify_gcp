# start colima

# Build and push the container to Google Container Registry
gcloud builds submit --tag gcr.io/dt-dta-adrien-sandbox-dev/dbt-streamify:latest

# Deploy a Cloud Run job
gcloud run jobs create dbt-streamify \
  --image gcr.io/dt-dta-adrien-sandbox-dev/dbt-streamify:latest \
  --region us-central1 

# Execute manually if needed
# gcloud run jobs execute dbt-streamify --region us-central1

# Docker Push to cloud REgistry
# Cloud Run utilise la derniere v de mon Container

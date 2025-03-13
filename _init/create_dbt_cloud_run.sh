source .env

# start colima

# Build and push the container to Google Container Registry
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${CLOUD_RUN_NAME}:latest

# Deploy a Cloud Run job
gcloud run jobs create ${CLOUD_RUN_NAME} \
  --image gcr.io/${PROJECT_ID}/${CLOUD_RUN_NAME}:latest \
  --region ${REGION}

# Execute manually if needed
# gcloud run jobs execute ${CLOUD_RUN_NAME} --region ${REGION}

# Docker Push to cloud REgistry
# Cloud Run utilise la derniere v de mon Container

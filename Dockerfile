# Use Python 3.12-slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy your dbt project files
COPY transform/. .

# Install dependencies
RUN pip install -r requirements.txt

# Install dbt pckages
RUN dbt deps

# Entrypoint to run dbt commands
CMD ["dbt", "build", "-t", "prod"]

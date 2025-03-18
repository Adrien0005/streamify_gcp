from google.cloud import bigquery
import pandas as pd

# Initialize BigQuery client
bq_client = bigquery.Client()

# Perform a BigQuery query
QUERY = (
    'SELECT * FROM `dt-dta-adrien-sandbox-dev.streamify_dbt_artifacts.model_executions` '
    'LIMIT 10'
)
query_job = bq_client.query(QUERY)
df = query_job.result().to_dataframe()

print(df)

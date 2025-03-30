## Load Data from GCS into BigQuery
from google.cloud import bigquery
import os

# Set credentials for Google Cloud authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/app/service_account.json"
def push_biquery_to():
    # Parameters
    project_id = "acoustic-env-454618-v3"
    dataset_id = "covid_raw_data"
    table_id = "vaccination_data_raw"
    uri = f"gs://covid_vaccine_bucket_de/vaccination_data.parquet"

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Fully qualified table ID (Project.Dataset.Table)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Load configuration for Parquet
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema_update_options=[bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION],  # Allows adding new columns
    )

    # Load data from GCS to BigQuery
    load_job = client.load_table_from_uri(uri, full_table_id, job_config=job_config)

    print(f"Starting job: {load_job.job_id}")

    # Wait for the job to complete
    load_job.result()

    print(f"✅ Data successfully loaded into {full_table_id}. Rows loaded: {load_job.output_rows}")
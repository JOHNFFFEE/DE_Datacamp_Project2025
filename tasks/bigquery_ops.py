from google.cloud import bigquery
from prefect import task, get_run_logger
from prefect_gcp import GcpCredentials

import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))  # Adjust based on your structure

from utils.config import Config


gcp_credentials_block = GcpCredentials.load("gcs")

# Load GCP credentials
try:
    gcp_credentials_block = GcpCredentials.load("gcs")
except Exception as e:
    logger = get_run_logger()
    logger.error(f"Failed to load GCP credentials: {str(e)}")
    raise

@task
def load_gcs_to_bigquery(gcs_uri: str) -> int:
    logger = get_run_logger()
    """
    Load data from GCS to BigQuery
    
    Args:
        gcs_uri: URI of the Parquet file in GCS (format: gs://bucket-name/file.parquet)
    
    Returns:
        Number of rows loaded
        
    Raises:
        Exception: If the load operation fails
    """
    try:
        # Get credentials and initialize client
        credentials = gcp_credentials_block.get_credentials_from_service_account()
        client = bigquery.Client(
            credentials=credentials,
            project=Config.PROJECT_ID
        )
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        table_ref = f"{Config.PROJECT_ID}.{Config.DATASET_ID}.{Config.TABLE_ID}"
        logger.info(f"Loading data from {gcs_uri} to {table_ref}")
        
        load_job = client.load_table_from_uri(
            source_uris=gcs_uri,
            destination=table_ref,
            job_config=job_config
        )
        
        load_job.result()
        table = client.get_table(table_ref)
        logger.info(f"Successfully loaded {table.num_rows} rows")
        
        return table.num_rows
        
    except Exception as e:
        logger.error(f"BigQuery load failed: {str(e)}")
        raise


if __name__ == "__main__":
    gcs_uri = f"gs://{Config.BUCKET_NAME}/vaccination_data.parquet"
    load_gcs_to_bigquery(gcs_uri)


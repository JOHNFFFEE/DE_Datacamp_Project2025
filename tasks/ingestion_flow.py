import dlt
import pandas as pd
from google.cloud import storage
import tempfile 
import os
from prefect import task, flow
from prefect_gcp import GcpCredentials
gcp_credentials_block = GcpCredentials.load("gcs")

import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))  

from utils.config import Config

# Constants
BUCKET_NAME = Config.BUCKET_NAME

# Task to fetch vaccination data
@task
def fetch_vaccination_data():
    url = Config.VACCINATION_URL
    df = pd.read_csv(url)
    return df

# Task to run the dlt pipeline and process data
@flow
def run_dlt_pipeline(df):
    pipeline = dlt.pipeline(
        pipeline_name="covid_vaccine_ingestion",
        destination="duckdb",
        dataset_name="raw_data",
    )

    @dlt.resource(name="vaccination_data")
    def vaccination_data():
        yield df

    pipeline.run(vaccination_data, write_disposition="replace")
    print("Data saved to Parquet successfully.")
    return pipeline.dataset(dataset_type="default").vaccination_data.df()

# Task to upload to GCS
@task
def upload_to_gcs(df):
    try:
        # Create temp file
        temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        temp_path = temp_file.name
        temp_file.close()  # Explicitly close the file handle
        
        # Write to parquet
        df.to_parquet(temp_path, engine="pyarrow")
        
        # Upload to GCS
        credentials = gcp_credentials_block.get_credentials_from_service_account()
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob("vaccination_data.parquet")
        blob.upload_from_filename(temp_path)
        
        print(f"Uploaded to gs://{BUCKET_NAME}/vaccination_data.parquet")
        return f"gs://{BUCKET_NAME}/vaccination_data.parquet"
        
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except PermissionError:
                print(f"Could not delete {temp_path} - it may be locked")
                # Optionally add a small delay and retry
                import time
                time.sleep(1)
                os.remove(temp_path)

# Define the Prefect Flow
@flow
def covid_vaccine_ingestion_flow():
    vaccination_df = fetch_vaccination_data()
    processed_df = run_dlt_pipeline(vaccination_df)
    upload_status = upload_to_gcs(processed_df)

# Run the flow
# if __name__ == "__main__":
    # covid_vaccine_ingestion_flow()
# ingestion_flow.py
import os
os.environ["PREFECT_API_V2"] = "true"
os.environ["PYDANTIC_V2"] = "true"
os.environ["PREFECT_RESULTS"] = "false"  # Disable problematic results feature

from prefect import flow, task
from tasks.ingestion_flow import covid_vaccine_ingestion_flow

from tasks.bigquery_ops import load_gcs_to_bigquery
from tasks.create_partitioned_table import create_partitioned_vaccination_table
import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))  # Adjust based on your structure

from utils.config import Config



@task (retries=4)
def ingestion():
    covid_vaccine_ingestion_flow()

@task
def gcs2bq():
    gcs_uri = f"gs://{Config.BUCKET_NAME}/vaccination_data.parquet"
    load_gcs_to_bigquery(gcs_uri)


@task
def createPartition():
    create_partitioned_vaccination_table()



@flow
def main_flow():
    # Fetch the vaccination data
    ingestion()
    gcs2bq()
    createPartition()


# Run the flow
if __name__ == "__main__":
    main_flow.serve(name="my-first-deployment", cron="0 12 * * *")
    
from google.cloud import bigquery
from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect import task, get_run_logger

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
def create_partitioned_vaccination_table():
    """Creates a partitioned and clustered vaccination data table in BigQuery"""
    
        # Get credentials and initialize client
    credentials = gcp_credentials_block.get_credentials_from_service_account()
    client = bigquery.Client(
        credentials=credentials,
        project=Config.PROJECT_ID
        )
    
    # Step 1: Create temporary table with proper date typing
    temp_table_query = """
    CREATE OR REPLACE TABLE `covid_raw_data.temp_vaccination`
    AS
    SELECT 
      * EXCEPT(`date`),
      CAST(`date` AS DATE) AS vaccination_date
    FROM `covid_raw_data.vaccination_data_raw`
    WHERE SAFE_CAST(`date` AS DATE) IS NOT NULL;
    """
    
    # Step 2: Create final partitioned and clustered table
    final_table_query = """
    CREATE OR REPLACE TABLE `covid_raw_data.vaccination_data_optimized`
    PARTITION BY vaccination_date
    CLUSTER BY location, iso_code
    AS 
    SELECT * FROM `covid_raw_data.temp_vaccination`;
    """

    drop_table= """
    drop table `acoustic-env-454618-v3.covid_raw_data.temp_vaccination`;
     """
    
    try:
        # Execute the queries
        print("Creating temporary table...")
        client.query(temp_table_query).result()
        
        print("Creating partitioned table...")
        client.query(final_table_query).result()

        print("Dropping temp table...")
        client.query(drop_table).result()
        
        print("Successfully created partitioned vaccination_data_optimized table")
        
    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        raise

# Run the function
# if __name__ == "__main__":
#     create_partitioned_vaccination_table()
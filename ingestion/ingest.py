import dlt
import pandas as pd
from google.cloud import storage
import os



# Ensure Google Cloud credentials are available
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/app/service_account.json"
def run_ingestion():

    # Define the dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="covid_vaccine_ingestion",
        destination="duckdb",
        dataset_name="raw_data",
        # destination="duckdb://covid_vaccine.duckdb",
    )

    # Define the data retrieval function using dlt.resource
    @dlt.resource(name="vaccination_data")
    def fetch_vaccination_data():
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"
        df = pd.read_csv(url)
        yield df  # Yielding the dataframe for efficient memory handling

    # Run the pipeline and store data in parquet
    pipeline.run(fetch_vaccination_data, write_disposition="replace")

    print("Data saved to Parquet successfully.")

    # explore loaded data
    df = pipeline.dataset(dataset_type="default").vaccination_data.df()

    # Save DataFrame to Parquet
    df = pipeline.dataset(dataset_type="default").vaccination_data.df()


    # Upload to Google Cloud Storage (GCS)


    # Paths and GCS configuration
    BUCKET_NAME = "covid_vaccine_bucket_de"
    LOCAL_FILE_PATH = "/usr/app/vaccination_data.parquet"  # Safer location in Docker
    GCS_FILE_PATH = "vaccination_data.parquet"

    # Save Parquet file
    df.to_parquet(LOCAL_FILE_PATH, engine="pyarrow")

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_PATH)

    blob.upload_from_filename(LOCAL_FILE_PATH)
    print(f"Data uploaded to gs://{BUCKET_NAME}/{GCS_FILE_PATH} successfully.")

    
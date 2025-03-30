from google.cloud import storage
import os

# Set credentials for Google Cloud authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/app/service_account.json"

# Set variables
BUCKET_NAME = "covid_vaccine_bucket_de"  # Replace with your GCS bucket name
LOCAL_FILE_PATH = "/tmp/vaccination_data.parquet"  # Parquet file generated from ingest.py
GCS_FILE_PATH = "vaccination_data.parquet"  # Destination in GCS

# Initialize Google Cloud Storage client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob(GCS_FILE_PATH)

# Upload file to GCS
print(f"Uploading {LOCAL_FILE_PATH} to gs://{BUCKET_NAME}/{GCS_FILE_PATH}...")

df.to_parquet(LOCAL_FILE_PATH, engine="pyarrow")
blob.upload_from_filename(LOCAL_FILE_PATH)
print("Data uploaded to GCS successfully.")
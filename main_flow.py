# ingestion_flow.py
from prefect import task, flow
import test



# Define the Prefect flow
@flow
def ingestion_flow():
    # Call the ingestion function
    test()

# Run the flow
if __name__ == "__main__":
    ingestion_flow()

    # Register the flow to make it appear in the UI
    # Deployment.build_from_flow(ingestion_flow).apply()cls
    
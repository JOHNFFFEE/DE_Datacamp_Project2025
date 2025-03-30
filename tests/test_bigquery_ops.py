import pytest
from unittest.mock import Mock, patch
from tasks.bigquery_ops import load_gcs_to_bigquery
from google.cloud import bigquery

@pytest.fixture
def mock_credentials():
    with patch('prefect_gcp.GcpCredentials.load') as mock:
        mock.return_value.get_credentials_from_service_account.return_value = "mock-creds"
        yield mock

@pytest.fixture
def mock_bigquery(mock_credentials):
    with patch('google.cloud.bigquery.Client') as mock:
        mock_instance = mock.return_value
        mock_table = Mock()
        mock_table.num_rows = 100
        mock_instance.get_table.return_value = mock_table
        yield mock_instance

def test_load_gcs_to_bigquery_success(mock_bigquery, mock_credentials):
    # Test with custom parameters
    row_count = load_gcs_to_bigquery(
        gcs_uri="gs://test-bucket/test.parquet",
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="test_table"
    )
    
    assert row_count == 100
    mock_bigquery.load_table_from_uri.assert_called_once()
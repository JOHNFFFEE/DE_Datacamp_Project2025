import pytest
from unittest.mock import Mock, patch
from tasks.ingestion_flow import fetch_vaccination_data, upload_to_gcs

@patch('ingestion_flow.pd.read_csv')
def test_fetch_vaccination_data(mock_read_csv):
    mock_df = Mock()
    mock_read_csv.return_value = mock_df
    
    result = fetch_vaccination_data()
    assert result == mock_df

@patch('ingestion_flow.storage.Client')
@patch('ingestion_flow.pd.DataFrame.to_parquet')
def test_upload_to_gcs(mock_to_parquet, mock_storage_client):
    mock_df = Mock()
    mock_bucket = Mock()
    mock_blob = Mock()
    
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    result = upload_to_gcs(mock_df)
    assert result.startswith("gs://")
    mock_to_parquet.assert_called_once()
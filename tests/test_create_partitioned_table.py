import pytest
from unittest.mock import Mock, patch
from tasks.create_partitioned_table import create_partitioned_vaccination_table

@pytest.fixture
def mock_bigquery_client():
    with patch('create_partitioned_table.bigquery.Client') as mock:
        yield mock

def test_create_partitioned_table_success(mock_bigquery_client):
    mock_client = Mock()
    mock_client.query.return_value.result.return_value = None
    mock_bigquery_client.return_value = mock_client
    
    create_partitioned_vaccination_table()
    
    assert mock_client.query.call_count == 3

def test_create_partitioned_table_failure(mock_bigquery_client):
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Test error")
    mock_bigquery_client.return_value = mock_client
    
    with pytest.raises(Exception):
        create_partitioned_vaccination_table()
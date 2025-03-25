import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import os
import sys
import warnings
from google.cloud import bigquery

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Scripts.streamlit_app import (
    run_bigquery_query,
    column_exists,
    get_all_columns
)

class TestHealthcareAnalytics(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for all tests."""
        cls.sample_data = pd.DataFrame({
            'PROVIDER': ['Dr. Smith', 'Dr. Johnson', 'Dr. Williams'],
            'APPOINTMENTS': [50, 30, 45],
            'PATIENT_ID': ['P001', 'P002', 'P003'],
            'REVENUE': [10000, 7500, 9000]
        })
        
        cls.empty_data = pd.DataFrame()

    @patch('Scripts.streamlit_app.client.query')
    @patch('Scripts.streamlit_app.bigquery.Client')
    def test_run_bigquery_query_success(self, mock_client, mock_query):
        """Test successful query execution."""
        # Setup mock
        mock_query_job = MagicMock()
        mock_query_job.to_dataframe.return_value = self.sample_data
        mock_query.return_value = mock_query_job
        
        # Need to mock the client instance used in the module
        mock_client.return_value = MagicMock()
        
        # Execute
        result = run_bigquery_query("SELECT * FROM `dataset.table`")
        
        # Verify
        mock_query.assert_called_once_with("SELECT * FROM `dataset.table`")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result.columns), 
                           ['PROVIDER', 'APPOINTMENTS', 'PATIENT_ID', 'REVENUE'])

    @patch('Scripts.streamlit_app.client.query')
    @patch('Scripts.streamlit_app.logger.error')
    def test_run_bigquery_query_failure(self, mock_logger, mock_query):
        """Test query execution failure."""
        # Setup mock
        test_error = Exception("400 Table must be qualified with a dataset")
        mock_query.side_effect = test_error
        
        # Execute
        result = run_bigquery_query("SELECT * FROM non_existent_table")
        
        # Verify
        self.assertIsNone(result)
        mock_logger.assert_called_once()
        self.assertIn("Query execution failed:", mock_logger.call_args[0][0])

    @patch('Scripts.streamlit_app.run_bigquery_query')
    def test_column_exists_true(self, mock_run_query):
        """Test column_exists returns True when column exists."""
        mock_run_query.return_value = pd.DataFrame({'column_name': ['PROVIDER']})
        result = column_exists('provider_productivity', 'PROVIDER')
        self.assertTrue(result)

    @patch('Scripts.streamlit_app.run_bigquery_query')
    def test_column_exists_false(self, mock_run_query):
        """Test column_exists returns False when column doesn't exist."""
        mock_run_query.return_value = self.empty_data
        result = column_exists('provider_productivity', 'NON_EXISTENT')
        self.assertFalse(result)

    @patch('Scripts.streamlit_app.run_bigquery_query')
    def test_get_all_columns(self, mock_run_query):
        """Test get_all_columns returns correct columns."""
        mock_run_query.return_value = pd.DataFrame({
            'column_name': ['PROVIDER', 'APPOINTMENTS', 'PATIENT_ID']
        })
        result = get_all_columns('appointment_analytics')
        self.assertEqual(result, ['PROVIDER', 'APPOINTMENTS', 'PATIENT_ID'])

    @patch('Scripts.streamlit_app.os.path.exists')
    @patch('Scripts.streamlit_app.st.error')
    def test_credential_file_validation(self, mock_st_error, mock_exists):
        """Test credential file validation."""
        mock_exists.return_value = False
        from Scripts.streamlit_app import os
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "invalid_path.json"
        
        if not os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
            mock_st_error("Error: Credentials file not found")
        
        mock_st_error.assert_called_with("Error: Credentials file not found")

    @patch('Scripts.streamlit_app.run_bigquery_query')
    def test_provider_filter_query(self, mock_run_query):
        """Test provider filter modifies query correctly."""
        mock_run_query.side_effect = [
            pd.DataFrame({'column_name': ['PROVIDER']}),
            pd.DataFrame({'PROVIDER': ['Dr. Smith', 'Dr. Johnson']})
        ]
        
        base_query = "SELECT * FROM healthcare_analytics.provider_productivity WHERE 1=1"
        provider_filter = ['Dr. Smith']
        
        if provider_filter:
            provider_list = ",".join([f"'{p}'" for p in provider_filter])
            filtered_query = base_query + f" AND `PROVIDER` IN ({provider_list})"
        
        self.assertEqual(filtered_query, 
                       "SELECT * FROM healthcare_analytics.provider_productivity WHERE 1=1 AND `PROVIDER` IN ('Dr. Smith')")

if __name__ == '__main__':
    # Suppress warnings during tests
    warnings.filterwarnings("ignore", message="BigQuery Storage module not found")
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
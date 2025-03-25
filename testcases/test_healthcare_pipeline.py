import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
import os
import streamlit as st

class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        """Set up sample data for testing"""
        self.patients = pd.DataFrame({
            'Id': ['P1', 'P2', 'P3'],
            'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [30, 40, 50]
        }).rename(columns={'Id': 'patient_id'})  # Ensure renaming

        self.encounters = pd.DataFrame({
            'PATIENT': ['P1', 'P2', 'P4'],  # P4 does not match any patient_id
            'PROVIDER': ['Dr. X', 'Dr. Y', 'Dr. Z'],
            'START': ['2024-01-01', '2024-02-01', '2024-03-01']
        }).rename(columns={'PATIENT': 'patient_id'})

    def test_column_renaming(self):
        """Test if columns are correctly renamed"""
        self.assertIn('patient_id', self.patients.columns)
        self.assertIn('patient_id', self.encounters.columns)

    def test_merge_dataframes(self):
        """Test merging patients and encounters"""
        print("Patients Columns:", self.patients.columns)
        print("Encounters Columns:", self.encounters.columns)

        # Ensure columns exist before merging
        self.assertIn('patient_id', self.patients.columns, "Missing patient_id in patients dataframe")
        self.assertIn('patient_id', self.encounters.columns, "Missing patient_id in encounters dataframe")

        # Perform merge
        merged_df = pd.merge(self.patients, self.encounters, on='patient_id', how='inner')

        # Check if merged correctly
        self.assertEqual(len(merged_df), 2)  # P4 should be excluded
        self.assertTrue('START' in merged_df.columns)

class TestBigQueryUpload(unittest.TestCase):
    @patch('google.cloud.bigquery.Client')
    def test_bigquery_upload(self, mock_bigquery_client):
        """Test BigQuery upload process"""
        mock_client = mock_bigquery_client.return_value
        dataset_id = 'healthcare_analytics'

        # Mock dataset check
        mock_client.get_dataset.side_effect = Exception("Dataset not found")
        dataset_ref = mock_client.dataset(dataset_id)

        # Try creating dataset
        try:
            mock_client.create_dataset(dataset_ref)
            dataset_created = True
        except Exception:
            dataset_created = False

        self.assertTrue(dataset_created, "Dataset creation failed!")

class TestStreamlitDashboard(unittest.TestCase):
    @patch('google.cloud.bigquery.Client')
    def test_load_bigquery_data(self, mock_bigquery_client):
        """Test loading data from BigQuery"""
        mock_client = mock_bigquery_client.return_value
        mock_client.query.return_value.to_dataframe.return_value = pd.DataFrame({
            'PROVIDER': ['Dr. X', 'Dr. Y'],
            'encounter_count': [10, 20]
        })

        dataset_id = 'healthcare_analytics'
        table_name = 'provider_productivity'
        query = f"SELECT * FROM `{dataset_id}.{table_name}`"
        df = mock_client.query(query).to_dataframe()

        self.assertFalse(df.empty, "Dataframe should not be empty")
        self.assertIn('PROVIDER', df.columns)
        self.assertIn('encounter_count', df.columns)

if __name__ == '__main__':
    unittest.main()

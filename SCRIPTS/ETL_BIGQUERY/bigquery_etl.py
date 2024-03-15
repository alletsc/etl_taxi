import logging
import os

import pandas as pd
from google.cloud import bigquery


class BigQueryETL:
    def __init__(self, credentials_path, dataset_id):
        self.credentials_path = credentials_path
        self.dataset_id = dataset_id
        self.client = self._create_bigquery_client()

    def _create_bigquery_client(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials_path
        return bigquery.Client()

    def _ensure_dataset_exists(self):
        try:
            self.client.get_dataset(self.dataset_id)
            logging.info(f"Dataset {self.dataset_id} already exists.")
        except:
            dataset = bigquery.Dataset(self.dataset_id)
            self.client.create_dataset(dataset)
            logging.info(f"Dataset {self.dataset_id} created.")

    def query_to_dataframe(self, query):
        df = self.client.query(query).to_dataframe()
        logging.info("Query executed and dataframe created.")
        return df

    def load_dataframe_to_bigquery(self, df, table_id):
        self._ensure_dataset_exists()
        df.to_gbq(table_id, if_exists='replace',
                  project_id=self.client.project)
        logging.info(f"Dataframe loaded to {table_id} in BigQuery.")

import os

from google.cloud import bigquery, storage


class BigQueryDataLoader:
    def __init__(self, project_id, bucket_name, dataset_id, credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client()

    def load_csv_to_bigquery(self, path_to_file, table_id, schema):
        uri = f'gs://{self.bucket_name}/{path_to_file}'
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE'
        )

        load_job = self.client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        print(f"Data loaded to {table_id} successfully.")

    def upload_file_to_bucket(self, local_file_path, destination_file_name):
        bucket = self.storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(destination_file_name)
        blob.upload_from_filename(local_file_path)
        print(f"File {local_file_path} uploaded to {
              destination_file_name} successfully.")

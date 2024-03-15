from loader import *

credentials_path = os.path.join(os.getcwd(), 'GCP/teste-etl-taxirio.json')
project_id = 'rj-iplanrio'
bucket_name = 'rj-iplanrio-taxirio-data'
dataset_id = 'transporte_taxirio_staging'

loader = BigQueryDataLoader(project_id, bucket_name,
                            dataset_id, credentials_path)

# ENVIAR ARQUIVOS DA PASTA DADOS PARA O BUCKET
path = os.path.join(os.getcwd(), 'dados')
for file in os.listdir(path):
    if file.endswith('.csv'):
        upload_file_path = os.path.join(path, file)
        loader.upload_file_to_bucket(upload_file_path, file)
        print(f"File {file} uploaded to {bucket_name} successfully.")

# CRIAR TABELAS NO BIGQUERY CONSIDERANDO TODOS OS CAMPOS COMO STRING
schema_cities = [
    bigquery.SchemaField("id_cidade", "STRING",
                         description="Identificador único da cidade."),
    bigquery.SchemaField("nome_cidade", "STRING",
                         description="Nome da cidade."),
    bigquery.SchemaField("sigla", "STRING", description="Sigla da cidade."),
]

schema_events = [
    bigquery.SchemaField("_id", "STRING", description=""),
    bigquery.SchemaField("name", "STRING", description=""),
    bigquery.SchemaField("description", "STRING", description=""),
    bigquery.SchemaField("startedBy", "STRING", description=""),
    bigquery.SchemaField("metrics", "STRING", description=""),
    bigquery.SchemaField("eventMetricsLabel", "STRING", description=""),
    bigquery.SchemaField("passengerLabel", "STRING", description=""),
    bigquery.SchemaField("driverLabel", "STRING", description=""),
    bigquery.SchemaField("backofficeLabel", "STRING", description=""),
    bigquery.SchemaField("corporativoLabel", "STRING", description=""),
    bigquery.SchemaField("whoStarted", "STRING", description=""),
]

schema_paymentmethods = [
    bigquery.SchemaField("_id", "STRING", description=""),
    bigquery.SchemaField("pindex", "STRING", description=""),
    bigquery.SchemaField("name", "STRING", description=""),
    bigquery.SchemaField("type", "STRING", description=""),
]


schema_races = [
    bigquery.SchemaField("column_name", "STRING", description=""),
    bigquery.SchemaField("_id", "STRING", description=""),
    bigquery.SchemaField("event", "STRING", description=""),
    bigquery.SchemaField("estimatedDuration", "STRING", description=""),
    bigquery.SchemaField("passenger", "STRING", description=""),
    bigquery.SchemaField("city", "STRING", description=""),
    bigquery.SchemaField("broadcastQtd", "STRING", description=""),
    bigquery.SchemaField("isSuspect", "STRING", description=""),
    bigquery.SchemaField("isInvalid", "STRING", description=""),
    bigquery.SchemaField("billing_associatedPaymentMethod",
                         "STRING", description=""),
    bigquery.SchemaField("totalToPay", "STRING", description=""),
    bigquery.SchemaField("totalPriceToll", "STRING", description=""),
    bigquery.SchemaField("totalWithDiscount", "STRING", description=""),
    bigquery.SchemaField("totalWithoutDiscount", "STRING", description=""),
    bigquery.SchemaField("status", "STRING", description=""),
    bigquery.SchemaField("createdAt", "STRING", description=""),
    bigquery.SchemaField(
        "geolocation_effective_origin_position_lat", "STRING", description=""),
    bigquery.SchemaField(
        "geolocation_effective_origin_position_lng", "STRING", description=""),
    bigquery.SchemaField(
        "geolocation_effective_destination_position_lng", "STRING", description=""),
    bigquery.SchemaField(
        "geolocation_effective_destination_position_lat", "STRING", description=""),
    bigquery.SchemaField("cancelledAt", "STRING", description=""),
    bigquery.SchemaField("car", "STRING", description=""),
    bigquery.SchemaField("driver", "STRING", description=""),
    bigquery.SchemaField("startedAt", "STRING", description=""),
    bigquery.SchemaField("finishedAt", "STRING", description=""),
    bigquery.SchemaField("reasonForInvalid", "STRING", description=""),
    bigquery.SchemaField("expiredAt", "STRING", description=""),
    bigquery.SchemaField("reasonForSuspect", "STRING", description=""),
    bigquery.SchemaField("score", "STRING", description=""),
    bigquery.SchemaField("associatedDiscount", "STRING", description=""),
    bigquery.SchemaField("billing_finalprice_totalToPay",
                         "STRING", description=""),
    bigquery.SchemaField("billing_finalprice_totalPriceToll",
                         "STRING", description=""),
    bigquery.SchemaField(
        "billing_finalprice_totalWithDiscount", "STRING", description=""),
    bigquery.SchemaField(
        "billing_finalprice_totalWithoutDiscount", "STRING", description=""),
]


loader.load_csv_to_bigquery(path_to_file='taxirio.cities.csv',
                            table_id='cidades',
                            schema=schema_cities)

loader.load_csv_to_bigquery(path_to_file='taxirio.events.csv',
                            table_id='events',  # TODO: Corrigir nome da tabela
                            schema=schema_events)


loader.load_csv_to_bigquery(path_to_file='taxirio.paymentmethods.csv',
                            table_id='metodos_pagamento',
                            schema=schema_paymentmethods)

loader.load_csv_to_bigquery(path_to_file='taxirio.races.csv',
                            table_id='corridas',
                            schema=schema_races)

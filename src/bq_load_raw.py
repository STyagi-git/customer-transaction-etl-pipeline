from google.cloud import bigquery

CUSTOMERS_SCHEMA = [
    bigquery.SchemaField("customer_id", "INT64"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("signup_date", "DATE"),
    bigquery.SchemaField("country", "STRING"),
]

TRANSACTIONS_SCHEMA = [
    bigquery.SchemaField("transaction_id", "INT64"),
    bigquery.SchemaField("customer_id", "INT64"),
    bigquery.SchemaField("transaction_ts", "TIMESTAMP"),
    bigquery.SchemaField("amount", "NUMERIC"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("merchant", "STRING"),
    bigquery.SchemaField("category", "STRING"),
]

def load_csv_to_bq(project_id: str, dataset: str, table: str, gcs_uri: str, schema):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )
    load_job.result()
    return table_id

def load_all(project_id: str, raw_dataset: str, uris: dict[str, str]):
    customers_table = load_csv_to_bq(
        project_id, raw_dataset, "customers_raw", uris["customers.csv"], CUSTOMERS_SCHEMA
    )
    transactions_table = load_csv_to_bq(
        project_id, raw_dataset, "transactions_raw", uris["transactions.csv"], TRANSACTIONS_SCHEMA
    )
    return {"customers_raw": customers_table, "transactions_raw": transactions_table}

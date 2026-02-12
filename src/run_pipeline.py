from src.config import get_settings
from src.gcs_upload import upload_batch
from src.bq_load_raw import load_all
from src.bq_run_sql import run_transformations

def main():
    s = get_settings()

    files = [
        "data/customers.csv",
        "data/transactions.csv",
    ]

    print("1) Uploading to GCS...")
    uris = upload_batch(s.gcs_bucket, files, s.gcs_prefix)
    for k, v in uris.items():
        print(f"   {k} -> {v}")

    print("2) Loading raw tables into BigQuery...")
    tables = load_all(s.project_id, s.bq_raw_dataset, uris)
    print("   Loaded:", tables)

    print("3) Running curated SQL transformations...")
    run_transformations(s.project_id, s.bq_raw_dataset, s.bq_curated_dataset)

    print("Done. Curated tables are ready:")
    print(f" - {s.project_id}.{s.bq_curated_dataset}.customer_summary")
    print(f" - {s.project_id}.{s.bq_curated_dataset}.daily_kpis")

if __name__ == "__main__":
    main()

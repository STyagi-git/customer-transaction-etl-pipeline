import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    project_id: str
    gcs_bucket: str
    bq_raw_dataset: str
    bq_curated_dataset: str
    gcs_prefix: str = "ingest"

def get_settings() -> Settings:
    project_id = os.environ.get("GCP_PROJECT_ID")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    bq_raw_dataset = os.environ.get("BQ_RAW_DATASET", "raw")
    bq_curated_dataset = os.environ.get("BQ_CURATED_DATASET", "curated")

    missing = [k for k,v in {
        "GCP_PROJECT_ID": project_id,
        "GCS_BUCKET": gcs_bucket
    }.items() if v is None]
    
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
    
    return Settings(
        project_id=project_id,
        gcs_bucket=gcs_bucket,
        bq_raw_dataset=bq_raw_dataset,
        bq_curated_dataset=bq_curated_dataset,
    )
from google.cloud import storage
from pathlib import Path
from datetime import datetime, timezone

def upload_file(bucket_name: str, local_path: str, destination_blob: str) -> str:
    """Uploads a file to Google Cloud Storage and returns the GCS URI."""
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    
    blob.upload_from_filename(local_path)
    
    return f"gs://{bucket_name}/{destination_blob}"

def upload_batch(bucket_name: str, files: list[str], prefix: str) -> dict[str, str]:
    """Uploads a batch of files to GCS under a common prefix."""
    
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    uris = {}
    
    for f in files:
        p = Path(f)
        dest = f"{prefix}/run_id={run_id}/{p.name}"
        uris[p.name] = upload_file(bucket_name, str(p), dest)
    
    return uris
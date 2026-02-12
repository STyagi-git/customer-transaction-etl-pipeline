from google.cloud import bigquery
from pathlib import Path

def render_sql(template_path: str, project: str, raw_dataset: str, curated_dataset: str) -> str:
    sql = Path(template_path).read_text(encoding="utf-8")
    sql = sql.replace("{{PROJECT}}", project)
    sql = sql.replace("{{RAW}}", raw_dataset)
    sql = sql.replace("{{CURATED}}", curated_dataset)
    return sql

def run_query(project_id: str, sql: str) -> None:
    client = bigquery.Client(project=project_id)
    job = client.query(sql)
    job.result()

def run_transformations(project_id: str, raw_dataset: str, curated_dataset: str) -> None:
    for path in [
        "sql/02_transform_customer_summary.sql",
        "sql/03_transform_daily_kpis.sql",
    ]:
        q = render_sql(path, project_id, raw_dataset, curated_dataset)
        run_query(project_id, q)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import urllib.request
from io import StringIO
from google.cloud import storage


# -----------------------------
# TASK LOGIC
# -----------------------------
def github_to_gcs():
    # 1️⃣ GitHub RAW CSV URL
    github_raw_url = (
        "https://raw.githubusercontent.com/"
        "ChetanBramhanwade/airflow-etl-prototype/main/data/employees.csv"
    )

    # 2️⃣ Read CSV from GitHub
    response = urllib.request.urlopen(github_raw_url)
    data = response.read().decode("utf-8")

    reader = csv.reader(StringIO(data))
    rows = list(reader)

    header = [col.lower() for col in rows[0]]
    records = rows[1:]

    # 3️⃣ Simple transformation
    processed_rows = []
    processed_date = datetime.utcnow().strftime("%Y-%m-%d")

    for row in records:
        processed_rows.append(row + [processed_date])

    header.append("processed_date")

    # 4️⃣ Convert back to CSV (in memory)
    output_buffer = StringIO()
    writer = csv.writer(output_buffer)
    writer.writerow(header)
    writer.writerows(processed_rows)

    # 5️⃣ Upload to GCS
    bucket_name = "chetan-etl-prototype-bucket"
    destination_path = "etl-output/employees_processed.csv"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_path)

    blob.upload_from_string(
        output_buffer.getvalue(),
        content_type="text/csv"
    )

    print("✅ ETL SUCCESS")
    print(f"📁 File written to: gs://{bucket_name}/{destination_path}")


# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="github_to_gcs_prototype",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["prototype", "github", "gcs", "etl"],
) as dag:

    github_to_gcs_task = PythonOperator(
        task_id="github_to_gcs",
        python_callable=github_to_gcs,
    )


# ETL Pipeline: GitHub to Google Cloud Storage
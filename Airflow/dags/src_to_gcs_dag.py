import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

# NOTE: there is no generic URL prefix after resource/, so we have to manually add them :(

subway_url_list = [
   "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/996cfe8d-fb35-40ce-b569-698d51fc683b/resource/441143ca-8194-44ce-a954-19f8141817c7/download/ttc-subway-delay-data-2022.xlsx",
   "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/996cfe8d-fb35-40ce-b569-698d51fc683b/resource/2fbec48b-33d9-4897-a572-96c9f002d66a/download/ttc-subway-delay-data-2023.xlsx"
]

streetcar_url_list = [
   "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/b68cb71b-44a7-4394-97e2-5d2f41462a5d/resource/28547222-35fe-48b6-ac4b-ccc67d286393/download/ttc-streetcar-delay-data-2022.xlsx",
   "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/b68cb71b-44a7-4394-97e2-5d2f41462a5d/resource/472d838d-e41a-4616-a11b-585d26d59777/download/ttc-streetcar-delay-data-2023.xlsx"
]

bus_url_list = [
   "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/e271cdae-8788-4980-96ce-6a5c95bc6618/resource/3b3c2673-5231-4aac-8b6a-dc558dce588c/download/ttc-bus-delay-data-2022.xlsx",
   "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/e271cdae-8788-4980-96ce-6a5c95bc6618/resource/10802a64-9ac0-4f2e-9538-04800a399d1e/download/ttc-bus-delay-data-2023.xlsx"
]

def extract_and_format(src_url, parquet_file):
    # read and store the contents of the file in a pandas df
    df = pd.read_excel(src_url)

    # Convert the 'Route'/'Line' column to numeric and drop rows where conversion fails
    if "bus" in src_url:
        df = df[pd.to_numeric(df['Route'], errors='coerce').notna()]
    elif "street" in src_url:
        df = df[pd.to_numeric(df['Line'], errors='coerce').notna()]

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)
    # Write PyArrow Table to Parquet file and save in the data dir
    pq.write_table(table, parquet_file)
 

# NOTE func name is not std, exec takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
   """
   Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
   :param bucket: GCS bucket name
   :param object_name: target path & file-name
   :param local_file: source path & file-name
   :return:
   """

   # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
   # (Ref: https://github.com/googleapis/python-storage/issues/74)
   storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
   storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
   # End of Workaround

   client = storage.Client()
   bucket = client.bucket(bucket)

   blob = bucket.blob(object_name)
   blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="src_to_gcs_dag",
    schedule_interval="0 6 1 * *",
    start_date=days_ago(1),
    default_args=default_args,
    max_active_runs=3,
) as dag:
    
    # temporary :
    install_openpyxl_task = BashOperator(
        task_id='install_openpyxl_task',
        bash_command='pip install openpyxl'
    )

    for type, url_list in [("bus", bus_url_list), ("streetcar", streetcar_url_list), ("subway", subway_url_list)]:
        for year in [2022, 2023]:

            year_str = str(year)
            year_url = next((url for url in url_list if year_str in url), None)

            if year_url:
                resource_id = year_url.split("resource/")[1].split("/download/")[0]
                exec_year = year
                parquet_file = f'{AIRFLOW_HOME}/ttc-{type}-delay-data-{exec_year}.parquet'

                extract_and_format_task = PythonOperator(
                    task_id=f"{type}_{year}_extract_and_format_task",
                    python_callable=extract_and_format,
                    op_kwargs={
                        "src_url": year_url,
                        "parquet_file": parquet_file,
                    },
                )

                local_to_gcs_task = PythonOperator(
                    task_id=f"{type}_{year}_local_to_gcs_task",
                    python_callable=upload_to_gcs,
                    op_kwargs={
                        "bucket": BUCKET,
                        "object_name": f"{type}_delay_data/ttc-{type}-delay-data-{exec_year}.parquet",
                        "local_file": parquet_file,
                    },
                )

                remove_files_task = BashOperator(
                    task_id=f"{type}_{year}_remove_files_task",
                    bash_command=f"rm {parquet_file}"
                )

                install_openpyxl_task >> extract_and_format_task >> local_to_gcs_task >> remove_files_task
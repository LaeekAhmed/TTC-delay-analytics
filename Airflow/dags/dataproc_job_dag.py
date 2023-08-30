import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)

from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

REGION = "us-east4"
ZONE = "us-east4-a"

CLUSTER_NAME = "ttc-da-cluster1"

main_file_name = 'spark_job.py'
source_path_file = '/opt/airflow/dags/' + main_file_name
# ↪ file : /opt/airflow/dags/spark_job.py to be uploaded to gcs bucket

gcs_path_file = f'code/{main_file_name}'
gcs_source_uri = f'gs://{BUCKET}/{gcs_path_file}'
# ↪ file : gs://ttc_data_lake_ttc-data-analytics/code/spark_job.py : main file for pyspark job

default_args = {
   "owner": "airflow",
   "depends_on_past": False,
   "retries": 0,
}

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

# DAG declaration - using a Context Manager (an implicit way)
with DAG(
   dag_id="dataproc_job_dag",
   schedule_interval="0 6 1 * *",
   start_date=days_ago(1),
   default_args=default_args,
   max_active_runs=1,
) as dag:

   upload_job_script_to_gcs_task = PythonOperator(
      task_id=f"upload_job_script_to_gcs_task",
      python_callable=upload_to_gcs,
      op_kwargs={
         "bucket": BUCKET,
         "object_name": gcs_path_file,
         "local_file": source_path_file,
      },
   )

   set_service_account_task = BashOperator(
      task_id='set_service_account_task',
   bash_command="""
   gcloud auth activate-service-account \
      ttt-da-user@ttc-data-analytics.iam.gserviceaccount.com \
      --key-file="/.google/credentials/ttc-data-analytics-key.json"   
   """,
   )

   create_cluster_task = BashOperator(
      task_id='create_cluster_task',
      bash_command=f"""
      gcloud dataproc clusters create {CLUSTER_NAME} \
         --region {REGION} \
         --zone {ZONE} \
         --single-node \
         --master-machine-type n2-standard-4 \
         --master-boot-disk-size 100 \
         --image-version 2.1-debian11 \
         --project {PROJECT_ID}
      """,
   )

   submit_job_task = BashOperator(
      task_id='submit_job_task',
      bash_command=f"""
      gcloud dataproc jobs submit pyspark \
         --cluster={CLUSTER_NAME} \
         --region={REGION} \
         --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
         {gcs_source_uri}
      """,
   )

   delete_cluster_task = DataprocDeleteClusterOperator(
      task_id="delete_cluster_task", 
      project_id=PROJECT_ID, 
      cluster_name=CLUSTER_NAME, 
      region=REGION,
   )

   upload_job_script_to_gcs_task >> set_service_account_task >> create_cluster_task >> submit_job_task >> delete_cluster_task
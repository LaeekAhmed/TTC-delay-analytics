import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 28),
    'retries': 1
}

dag = DAG('my_bash_dag', default_args=default_args, schedule_interval=None)

# Define the multiline Bash command
multi_line_bash_command = """
echo "This is line 1"
echo "This is line 2"
echo "This is line 3"
"""

set_service_account_cmd="""
gcloud auth activate-service-account \
   ttt-da-user@ttc-data-analytics.iam.gserviceaccount.com \
   --key-file="/.google/credentials/ttc-data-analytics-key.json"
"""

# Create a BashOperator with the multiline command
bash_task = BashOperator(
   task_id='my_bash_task',
   bash_command="""
   gcloud auth activate-service-account \
      ttt-da-user@ttc-data-analytics.iam.gserviceaccount.com \
      --key-file="/.google/credentials/ttc-data-analytics-key.json"   
   """,
   dag=dag
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

REGION = "us-east4"
ZONE = "us-east4-a"

CLUSTER_NAME = "ttc-da-cluster1"

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
   dag=dag
)


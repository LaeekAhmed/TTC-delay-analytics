*Description :* *todo*
[DE Zoomcamp Projects - GHCN-D by Marcos Jiménez - YouTube](https://www.youtube.com/watch?v=D9cQOefe5zA&list=PL3MmuxUbc_hKVX8VnwWCPaWlIHf1qmg8s&index=26&ab_channel=DataTalksClub%E2%AC%9B)
## 1. Setup + Ideas

Data source - flight data :

[OpenFlights: Airport and airline data](https://openflights.org/data.html)
[Open Data Catalogue - City of Toronto Open Data Portal](https://open.toronto.ca/catalogue/?search=ttc%20delay%20data&sort=score%20desc)

|Field Name|Description|Example|
|---|---|---|
|Report Date|The date (YYYY/MM/DD) when the delay-causing incident occurred|6/20/2017|
|Route|The number of the bus route|51|
|Time|The time (hh:mm:ss AM/PM) when the delay-causing incident occurred|12:35:00 AM|
|Day|The name of the day|Monday|
|Location|The location of the delay-causing incident|York Mills Station|
|Incident|The description of the delay-causing incident|Mechanical|
|Min Delay|The delay, in minutes, to the schedule for the following bus|10|
|Min Gap|The total scheduled time, in minutes, from the bus ahead of the following bus|20|
|Direction|The direction of the bus route where B,b or BW indicates both ways. <br>(On an east west route, it includes both east and west)<br>NB - northbound, SB - southbound, EB - eastbound, WB - westbound|N||
|Vehicle|Vehicle number|1057|

Processing Batch - Every 1 hour

**Insight ideas (highly depends on source) :**

- Maps can be used to display flight density
- Most popular destinations
- Most popular routes
- Most flown aircrafts
- Busiest airports
- Most no. of flights of an Airline

Github repo (to be cloned in the VM)
variables can be added to a `.env` file or can be added to the VM based on [setup.sh](https://github.com/MarcosMJD/ghcn-d/blob/main/setup.sh) and [streamify/scripts/vm_setup.sh](https://github.com/ankurchavda/streamify/blob/main/scripts/vm_setup.sh)
Architecture diagram 

---
## 2. Infrastructure

Terraform
- It is recommended to use terraform to setup all gcp services so that you can destroy them once done with the project, to avoid costs
- But since this has to be hosted on a site, we will have to keep it alive and thus pay the corresponding charges
- We can save the image of the VM and can use that while creating an VM instance and can pass that image as a param for that VM
- Change `backend` to preserve your tf-state online, requires creating a bucket in gcs to store the `.tfstate` file
```python
terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google)
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}
```

- Airflow runs inside the provided docker container but instead of local machine, we run it on a virtual machine in gcp
- The same machine runs terraform [machine + software setup](https://www.youtube.com/redirect?event=video_description&redir_token=QUFFLUhqbG4wSlpZY0pGR1VLSVN5Ny1ydktMd050cXNiUXxBQ3Jtc0tsM201ZE5xRXd4bTVWT2VNTDllYlpWeXBtaDhHZlVleXMyZ0ZWa1NmelVQNnpCMWtBSW1jbGE3WTFWQ3UxUHJURWF3U0F3LUVJb1B2b005YVV3MER0eHNwVGJxWmNWQkxkMmEzNUdlbWRoeGlRSk52QQ&q=https%3A%2F%2Fgithub.com%2FMarcosMJD%2Fghcn-d%2Fblob%2Fmain%2Fsetup_vm.md&v=D9cQOefe5zA)
- This machine can be created using Terraform to enhance automation
- In order to control cost and resources in DataProc, a DataProc cluster is created and deleted in each dag execution. [ghcn-d/README.md at main · MarcosMJD/ghcn-d (github.com)](https://github.com/MarcosMJD/ghcn-d/blob/main/README.md)
- Airflow can also create tables using BigQuery operators, so we leave that to it
- Destroying a dataset will also remove all the tables so we don't have to worry about that
- objects within the Google Cloud Storage bucket are automatically deleted after they have been present for 30 days due to the `lifecycle_rule`

---
## 3. ELT

#### 3.1 Airflow

Dags + their diagrams
Check if the DAGs are running automatically as per the schedule and **don't** require manual trigger via Web UI

---
#### 3.2 Extract 

Spark ? python ? source + gcs

---
#### 3.3 Load

gcs + BigQuery

---
#### 3.4 Transformations

dbt ? spark ?

while downloading spark, refer https://spark.apache.org/downloads.html which will redirect to https://www.apache.org/dyn/closer.lua/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz (the actual downloadable file!)

dbt jobs can be run manually from the web ui or can be orchestrated using airflow
we use the API option under job settings :

![[Pasted image 20230820221913.png]]

Get the **API key** from Profile Settings and put in a file so that we don't have to pass the entire key contents and can just pass the file instead

We can get the **Job Id**, **Project Id** and the **Account Id** from the same page

Check out [Airflow and dbt Cloud | dbt Developer Hub (getdbt.com)](https://docs.getdbt.com/guides/orchestration/airflow-and-dbt-cloud/1-airflow-and-dbt-cloud), we have a `DbtCloudRunJobOperator()` to do run dbt jobs from Airflow dags!

The DataProc cluster can be deleted after it finished the spark jobs so as to avoid running charges, we have Operators for that, 
refer [airflow/dags/dataproc_spark_job_dag.py(github.com)](https://github.com/MarcosMJD/ghcn-d/blob/main/airflow/dags/dataproc_spark_job_dag.py)

---
## 4. Visualization

looker studio

---
## 5. Further Updates

- Look into cloud composer to automate ?? its a GCP service to create airflow instances ??
- Stream processing
- Update Flypedia site 
- Change `backend` to preserve your tf-state online, requires creating a bucket in gcs to store the `.tfstate` file

## 6. How to run
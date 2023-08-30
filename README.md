# TTC Delay Analytics

Analyzing TTC bus, subway & streetcar delay data to identify delay hotspots, root causes, and find valuable insights by creating an ETL (Extract, Transform, Load) pipeline and using Looker Studio to create dashboards.

![diagram](images/image.png){:target="_blank"}

## Source data

Data source - TTC bus, subway. streetcar delay data : [Open Data Catalogue - City of Toronto Open Data Portal](https://open.toronto.ca/catalogue/?search=ttc%20delay%20data&sort=score%20desc)

### Understading the source data

> Subway

|Field Name|Description|Example|
|---|---|---|
|Date|Date (YYYY/MM/DD)|12/31/2016|
|Time|Time (24h clock)|1:59|
|Day|Name of the day of the week|Saturday|
|Station|TTC subway station name|Rosedale Station|
|Code|TTC delay code|MUIS|
|Min Delay|Delay (in minutes) to subway service|5|
|Min Gap|Time length (in minutes) between trains|9|
|Bound|Direction of train dependant on the line|N|
|Line|TTC subway line i.e. YU, BD, SHP, and SRT|YU|
|Vehicle|TTC train number|5961|

> Bus & Streetcar

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

---
## Infrastructure

This setup creates VM nstance using gcp and will perform all tasks using the VM.

**(Note)** Terraform has to be used in a local setup as it the VM will be created using terraform

Architecture Diagram (ETL pipeline) :

![diagram](images/image.png){:target="_blank"}

- We will use **Terraform** to setup the infrastructure 
- We will use a **VM instance** to run everything
- We will use **Docker** to run Airflow inside the VM
- We will use **Airflow** to orchestrate the entire pipeline
- We will **gcs** as our data lake and **BigQuery** as our data warehouse
- We will use Spark to process, transform and clean the data

> **Processing** : Batch using spark   
**Frequency** : Every year (scheduled using Airflow)   
Can also be increased to every month but that is not cost effective

Airflow Dags :

![Alt text](images/image-5.png){:target="_blank"}

[src_to_gcs_dag.py](Airflow/dags/src_to_gcs_dag.py)
downloads data from the Toronto open portal website, converts it to parquet form, loads it into gcs bucket and removes it from local folder once it is available on the cloud.

![Alt text](images/image-6.png){:target="_blank"}

[dataproc_job_dag.py](Airflow/dags/dataproc_job_dag.py) uploads the main python file [spark_job.py](Airflow/dags/spark_job.py) for running the spark job to gcs bucket so dataproc can fetch it, set the service account which authorizes the airflow container to work with gcp services, create a cluster, run the job and then delete it (saves cost)

## Dashboard

[looker studio dashboard ↗](https://lookerstudio.google.com/reporting/c2f8e496-b46b-4f07-8025-635abf038a21)

![Alt text](images/image-3.png){:target="_blank"}

There are no delay records for July - December 2023 as this image was taken in August 2023 

![Alt text](images/image-4.png){:target="_blank"}
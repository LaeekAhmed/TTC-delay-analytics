# First-time build can take few mins.

FROM apache/airflow:2.6.3

ENV AIRFLOW_HOME=/opt/airflow

# Compulsory to switch parameter
ENV PIP_USER=false

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

#python venv setup
RUN python3 -m venv /opt/airflow/venv1

COPY requirements.txt .
# RUN pip install --upgrade pip
RUN /opt/airflow/venv1/bin/pip install --no-cache-dir -r requirements.txt
ENV PIP_USER=true

## jdk files and Variables ####################################################

RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin/:${PATH}"

## Finished jdk files and Variables

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]


## GCP SDK files and Variables ###################################################

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

## Finished GCP SDK files and variables


## Spark files and Variables ######################################################

ARG SPARK_VERSION=3.3.3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin/:${PATH}"

# https://dlcdn.apache.org/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz

RUN DOWNLOAD_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/spark.tgz" \
    && mkdir -p "${SPARK_HOME}" \
    && tar xzf "${TMP_DIR}/spark.tgz" -C "${SPARK_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}"

## Pyspark setup ###################################################################

ENV PYTHONPATH="/usr/local/bin/python"
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

## Finished Spark files and Variables

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID
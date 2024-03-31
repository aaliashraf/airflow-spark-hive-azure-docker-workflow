FROM apache/airflow:2.8.4-python3.10

LABEL maintainer="ali_ashraf"

USER root:root


RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

USER airflow

RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow

WORKDIR /opt/airflow

RUN pip install -r requirements.txt
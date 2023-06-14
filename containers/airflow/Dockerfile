FROM apache/airflow:2.10.2
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY airflow_conn.py $AIRFLOW_HOME

USER root

RUN apt-get update && \
    apt-get install -y unzip


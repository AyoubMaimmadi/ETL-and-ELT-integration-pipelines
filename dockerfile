FROM apache/airflow:latest



USER root

RUN apt-get update && \

    apt-get -y install git && \

    apt-get clean



USER airflow

# Install Python dependencies for ELT_automated.py
RUN pip install package1 package2 package3
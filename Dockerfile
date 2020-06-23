FROM python:3.6

ENV DEBIAN_FRONTEND noninteractive
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW_HOME=/usr/local/airflow

RUN echo 1 > /dev/null
RUN apt-get update -yqq
RUN apt-get upgrade -yqq

RUN useradd -ms /bin/bash -d /usr/local/airflow airflow && \
    pip install -U pip setuptools wheel && \
    pip install pytz pyOpenSSL ndg-httpsclient pyasn1 awscli boto3
RUN pip install apache-airflow[amazon,gcp,crypto,celery,postgres,hive,jdbc,ssh,redis,dynamodb,rabbitmq]
RUN pip install apache-airflow-backport-providers-amazon
RUN pip install celery==4.3.0
RUN pip install psycopg2-binary==2.8.5
RUN pip install pyamqp


# Put other custom packages here #

USER root
WORKDIR $AIRFLOW_HOME
RUN chown -R airflow:airflow ${AIRFLOW_HOME}
EXPOSE 8080 5555 8793 8000
USER airflow
ENTRYPOINT ["airflow"]

Airflow Run
================

Python tool for deploying Airflow Multi-Node Cluster (a.k.a. Celery Executor Setup).

Requirements
------------

* Python >=3.6 (tested)

Installation
------------
```
    pip install airflow-run
```

Goal
----

To provide a quick way to setup Airflow Multi-Node Cluster (a.k.a. Celery Executor Setup).

Requirements:
-------------
1. Docker

Setup
-----
1. Update the followings in config.yaml file.

  a. local_dir

  b. username password for postgresql and rabbitmq.

  c. host (IP) for postgresql and rabbitmq.

2. Run the following commands to start Rabbitmq, Postgresql and other Airflow services:

Generate config file:
---------------------
Run the following and follow the prompt to generate config file.
```
afr --generate_config_file
```

Running the tool:
-----------------
```
afr --config_path={path to config.yaml} --run postgresql
afr --config_path={path to config.yaml} --run rabbitmq
afr --config_path={path to config.yaml} --run webserver
afr --config_path={path to config.yaml} --run scheduler
afr --config_path={path to config.yaml} --run worker --queue {queue name}
afr --config_path={path to config.yaml} --run flower
```


Default Config yaml file:
-------------------------
```
private_registry: False
registry_url: registry.hub.docker.com
username: "" # username for logging in  private registry
password: "" # password for logging in private registry
repository: pkuong/airflow-run
image: airflow-run
tag: latest
local_dir: {local directory where you want to mount /dags and /logs folder}
webserver_port: 8000
flower_port: 5555
airflow_cfg:
  AIRFLOW__CORE__EXECUTOR: CeleryExecutor
  AIRFLOW__CORE__LOAD_EXAMPLES: "False"
  AIRFLOW__CORE__DAGS_FOLDER: /usr/local/airflow/airflow/dags # /dags directory in container
  AIRFLOW__CORE__LOGS_FOLDER: /usr/local/airflow/airflow/logs # /logs directory in container
  AIRFLOW_HOME: /usr/local/airflow
rabbitmq:
  name: rabbitmq
  username: {username}
  password: {password}
  host: {IP}
  virtual_host: /
  image: rabbitmq:3-management
  home: /var/lib/rabbitmq
  ui_port: 15672
  port: 5672
postgresql:
  name: postgresql
  username: {username}
  password: {password}
  host: {host}
  image: postgres
  data: /var/lib/postgresql/data
  port: 5432
  env:
    PGDATA: /var/lib/postgresql/data/pgdata
    POSTGRES_PASSWORD: {password}
```

Docker image
------------
This tool is using the following public docker image by default.
```
https://hub.docker.com/repository/docker/pkuong/airflow-run
```

Building the image:
-------------------
If you want to build your own image, you can run the following:

```python
afd --build --config_path={absolute path to config.yaml} --dockerfile_path={absolute path to directory which contains Dockerfile}
```


Contributors
------------

* Paulo Kuong ([@pkuong](https://github.com/paulokuong))

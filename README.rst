Airflow Run
----------------

Python tool for deploying Airflow Multi-Node Cluster.

Requirements
------------

-  Python >=3.6 (tested)

Goal
----

| To provide a quick way to setup Airflow Multi-Node Cluster (a.k.a. Celery Executor Setup).

Steps
-----
1. Generate config yaml file.
2. Run commands to start Rabbitmq, Postgresql and other Airflow services:

Generate config file:
------------------------

.. code:: python

    afr --generate_config_file

Running the tool:
--------------------

.. code:: python

    afr --config_path={path to config.yaml} --run postgresql
    afr --config_path={path to config.yaml} --run rabbitmq
    afr --config_path={path to config.yaml} --run webserver
    afr --config_path={path to config.yaml} --run scheduler
    afr --config_path={path to config.yaml} --run worker --queue {queue name}
    afr --config_path={path to config.yaml} --run flower

Default Config yaml file:
-------------------------

.. code-block:: yaml

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


Docker image
------------

| This tool is using the following public docker image by default.

.. code:: python

    https://hub.docker.com/repository/docker/pkuong/airflow-run

Building the image:
-------------------

| If you want to build your own image, you can run the following:

.. code:: python

    afd --build --config_path={absolute path to config.yaml} --dockerfile_path={absolute path to directory which contains Dockerfile}

Contributors
------------

-  Paulo Kuong (`@pkuong`_)

.. _@pkuong: https://github.com/paulokuong

.. |Build Status| image:: https://travis-ci.org/paulokuong/airflow-run.svg?branch=master
.. target: https://travis-ci.org/paulokuong/airflow-run

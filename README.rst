| |Build Status|

Airflow Run
----------------

Python tool for deploying Airflow Multi-Node Cluster.

Requirements
------------

-  Python >=3.6 (tested)

Goal
----

| To provide an easy way to deploy Airflow Multi-Node Cluster.

Code sample
-----------

| To build the image

.. code:: python

    afd --build --config_path={absolute path to config.yaml} --dockerfile_path={absolute path to directory which contains Dockerfile}


Contributors
------------

-  Paulo Kuong (`@pkuong`_)

.. _@pkuong: https://github.com/paulokuong

.. |Build Status| image:: https://travis-ci.org/paulokuong/airflow-run.svg?branch=master
.. target: https://travis-ci.org/paulokuong/airflow-run

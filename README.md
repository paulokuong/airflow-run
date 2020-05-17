Airflow Run
================

Python tool for deploying Airflow Multi-Node Cluster.

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

To provide an easy way to deploy Airflow Multi-Node Cluster.

Code sample
-----------

To build the image:

```python
afd --build --config_path={absolute path to config.yaml} --dockerfile_path={absolute path to directory which contains Dockerfile}
```


Contributors
------------

* Paulo Kuong ([@pkuong](https://github.com/paulokuong))

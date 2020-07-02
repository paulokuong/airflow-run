from setuptools import find_packages
from setuptools import setup

with open('README.rst') as file:
    long_description = file.read()

setup(name='airflow-run',
      version='0.1.7',
      description=(
          'Simplified Airflow CLI Tool for Lauching CeleryExecutor Deployment'),
      install_requires=[
          'docker==4.2.0',
          'pyyaml==5.3.1',
          'sqlalchemy==1.3.16',
          'pika==1.1.0',
          'psycopg2-binary==2.8.5'
      ],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.4'
      ],
      keywords=(
          'Airflow CeleryExecutor distributed portable deployment runner '
          'docker kubernetes'),
      url='https://github.com/paulokuong/airflow-run',
      author='Paulo Kuong',
      author_email='paulo.kuong@gmail.com',
      license='MIT',
      packages=find_packages(exclude=["tests"]),
      include_package_data=True,
      zip_safe=False,
      long_description=long_description,
      entry_points=dict(console_scripts=[
          'airflow_run = airflow_run.run:cli',
          'afr = airflow_run.run:cli'
      ]))

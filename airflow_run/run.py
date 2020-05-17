import docker
import os
import yaml
import argparse
from sqlalchemy import create_engine


class AirflowRun(object):
    def __init__(self, config):
        """Constructor

        Args:
            config (str): path to config.yaml file.
        """
        with open(os.path.realpath(config), "r") as ymlfile:
            self.config = yaml.safe_load(ymlfile)
            self.client = docker.from_env()
            self.client.login(
                registry=self.config['registry_url'],
                username=self.config['username'],
                password=self.config['password'])

    def check_db_connection(self):
        """Check if postgresql can be connected.
        Bubble up exception when fails.
        """
        db_string = self.config['airflow_cfg']['AIRFLOW__CORE__SQL_ALCHEMY_CONN']
        db = create_engine(db_string)
        db.table_names()

    def check_rabbitmq_connection(self):
        """Check Rabbitmq connection."""
        credentials = pika.PlainCredentials(
            self.config['rabbitmq']['username'],
            self.config['rabbitmq']['password'])
        parameters = pika.ConnectionParameters(
            host=self.config['rabbitmq']['host'],
            port=self.config['rabbitmq']['port'],
            virtual_host=self.config['rabbitmq']['virtual_host'],
            credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        if connection.is_open:
            print('Rabbitmq is: OK')
            connection.close()
            return True

    def pull(self):
        """Pull image"""
        self.client.images.pull(
            "{registry_url}/{repository}".format(
                registry_url=self.config['registry_url'],
                repository=self.config['repository']),
            tag=self.config['tag'])

    def build(self, dockerfile):
        """Build and push airflow docker image

        Args:
            dockerfile (str): full path to Dockerfile.
        """
        print(self.client.images.build(
            path=dockerfile, buildargs=self.config['airflow_cfg'],
            tag=self.config['image']))
        image = self.client.images.get(self.config['image'])
        image.tag(
            repository='{}/{}'.format(
                self.config['registry_url'], self.config['repository']),
            tag=self.config['tag'])
        print(self.client.images.push(
            '{}/{}'.format(
                self.config['registry_url'], self.config['repository']),
            tag=self.config['tag']))

    def list(self):
        """List all containers

        Returns:
            list of contianers.
        """
        return [i.name for i in self.client.containers.list()]

    def kill(self, command="airflow_server"):
        """Kill container by name.

        Args:
            command (str): container name.
        """
        for container in self.client.containers.list():
            if container.name == command:
                container.kill()

    def _get_run_dict(self, name, command, ports=[], detach=True):
        environment = [
            "{}={}".format(k, v)
            for k, v in self.config['airflow_cfg'].items()
        ]
        if 'AIRFLOW__CORE__SQL_ALCHEMY_CONN' not in self.config[
                'airflow_cfg'].items():
            environment.append(
                ("AIRFLOW__CORE__SQL_ALCHEMY_CONN="
                 "postgresql+psycopg2://{}:{}@{}:{}/postgres").format(
                    self.config['postgresql']['username'],
                    self.config['postgresql']['password'],
                    self.config['postgresql']['host'],
                    self.config['postgresql']['port']
                ))
        if 'AIRFLOW__CELERY__RESULT_BACKEND' not in self.config[
                'airflow_cfg'].items():
            environment.append(
                ("AIRFLOW__CELERY__RESULT_BACKEND="
                 "db+postgresql://{}:{}@{}:{}/postgres").format(
                    self.config['postgresql']['username'],
                    self.config['postgresql']['password'],
                    self.config['postgresql']['host'],
                    self.config['postgresql']['port']
                ))
        if 'AIRFLOW__CELERY__BROKER_URL' not in self.config[
                'airflow_cfg'].items():
            environment.append(
                ("AIRFLOW__CELERY__BROKER_URL="
                 "pyamqp://{}:{}@{}:{}/postgres").format(
                    self.config['rabbitmq']['username'],
                    self.config['rabbitmq']['password'],
                    self.config['rabbitmq']['host'],
                    self.config['rabbitmq']['port'],
                    self.config['rabbitmq']['virtual_host']
                ))
        output = dict(
            image='{registry_url}/{repository}:{tag}'.format(
                registry_url=self.config['registry_url'],
                repository=self.config['repository'],
                tag=self.config['tag']),
            name=name,
            auto_remove=True,
            detach=detach,
            environment=,
            volumes={
                '{}/dags'.format(self.config['local_dir']): {
                    'bind': self.config['airflow_cfg']['AIRFLOW__CORE__DAGS_FOLDER'],
                    'mode': 'rw'
                },
                '{}/logs'.format(self.config['local_dir']): {
                    'bind': self.config['airflow_cfg']['AIRFLOW__CORE__LOGS_FOLDER'],
                    'mode': 'rw'
                }
            },
            command=command)
        if ports:
            output.update(ports={'{}/tcp'.format(p): p for p in ports})
        return output

    def start_postgresql(self):
        """Start postgres instance.
        """
        return self.client.containers.run(
            image=self.config['postgresql']['image'],
            name=self.config['postgresql']['name'],
            detach=True, auto_remove=True,
            ports={
                '{}/tcp'.format(p): p
                for p in [
                    self.config['postgresql']['port']]
            },
            environment=[
                "{}={}".format(k, v)
                for k, v in self.config['postgresql']['env'].items()
            ],
            volumes={
                '{}/{}'.format(self.config['local_dir'], '/postgresql'): {
                    'bind': self.config['postgresql']['data'],
                    'mode': 'rw'
                }
            }
        )

    def start_rabbitmq(self):
        """Docker run rabbitmq default image."""
        return self.client.containers.run(
            image=self.config['rabbitmq']['image'],
            name=self.config['rabbitmq']['name'],
            detach=True, auto_remove=True,
            ports={
                '{}/tcp'.format(p): p
                for p in [
                    self.config['rabbitmq']['ui_port'],
                    self.config['rabbitmq']['port']]
            },
            volumes={
                '{}/rabbitmq'.format(self.config['local_dir']): {
                    'bind': self.config['rabbitmq']['home'],
                    'mode': 'rw'
                }
            }
        )

    def start_webserver(self, name='airflow_server', detach=True):
        """Docker run airflow webserver.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        self.check_db_connection()
        self.check_rabbitmq_connection()
        return self.client.containers.run(
            **self._get_run_dict(name, [
                "webserver", "-p",
                str(self.config['webserver_port'])
            ], [self.config['webserver_port']], detach=detach))

    def start_scheduler(self, name='airflow_scheduler', detach=True):
        """Docker run airflow scheduler.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        self.check_db_connection()
        self.check_rabbitmq_connection()
        return self.client.containers.run(
            **self._get_run_dict(name, ["scheduler"], detach=detach))

    def start_worker(self, queue, name='airflow_worker', detach=True):
        """Docker run airflow worker.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        self.check_db_connection()
        self.check_rabbitmq_connection()
        return self.client.containers.run(
            **self._get_run_dict(name, ["worker", "-q", queue], detach=True))

    def start_flower(self, name='airflow_flower', detach=True):
        """Docker run airflow worker.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        self.check_db_connection()
        self.check_rabbitmq_connection()
        return self.client.containers.run(
            **self._get_run_dict(name, [
                "flower", "-p", str(self.config['flower_port'])
            ], [self.config['flower_port']], detach=True))

    def initdb(self, detach=True):
        """Docker run airflow initdb
        Args:
            detach (bool[optional]): True for detach container.
        """
        self.check_db_connection()
        return self.client.containers.run(
            **self._get_run_dict('initdb', ["initdb"], detach=True))


def cli():
    parser = argparse.ArgumentParser(description='Airflow Run')
    parser.add_argument(
        '--build', dest='build', action='store_true',
        help='Path to the Dockerfile.')
    parser.add_argument(
        '--list', dest='list', action='store_true',
        help='List all running services.')
    parser.add_argument(
        '--config', dest='config',
        help='Specify path to config yaml file.', required=True)
    parser.add_argument(
        '--dockerfile', dest='dockerfile',
        help='Path to the Dockerfile.')
    parser.add_argument(
        '--run', dest='run',
        help='command name: (webserver, rabbitmq, scheduler, worker, flower, initdb, postgresql)')
    parser.add_argument(
        '--queue', dest='queue',
        help='Queue name for the worker.')
    parser.set_defaults(queue='default')
    args = parser.parse_args()

    if not args.build and not args.run and not args.list:
        parser.print_help()
    if args.build:
        a = AirflowRun(args.config)
        if not args.dockerfile:
            raise Exception('--dockerfile is required.')
        a.build(args.dockerfile)
    elif args.list:
        a = AirflowRun(args.config)
        print(a.list())
    elif args.run:
        a = AirflowRun(args.config)
        a.client.containers.prune()
        if args.run == "rabbitmq":
            print(a.start_rabbitmq())
        elif args.run == "postgresql":
            print(a.start_postgresql())
        elif args.run == "webserver":
            print(a.start_webserver())
        elif args.run == "scheduler":
            print(a.start_scheduler())
        elif args.run == "worker":
            print(a.start_worker(args.queue))
        elif args.run == "flower":
            print(a.start_flower())
        elif args.run == "initdb":
            print(a.initdb())
        else:
            raise Exception(
                "--run param is invalid (Valid options are: rabbitmq, webserver, "
                "scheduler, worker, flower, initdb, postgresql).")
    else:
        parser.print_help()

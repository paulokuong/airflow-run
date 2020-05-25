import docker
import os
import yaml
import argparse
from sqlalchemy import create_engine
import pika
import time


class AirflowRun(object):
    def __init__(self, config: str):
        """Constructor

        Args:
            config (str): path to config.yaml file.
        """
        self.supported_services = [
            'flower', 'initdb', 'postgresql', 'rabbitmq', 'scheduler',
            'webserver', 'worker']
        with open(os.path.realpath(config), "r") as ymlfile:
            self.config = yaml.safe_load(ymlfile)
            self.client = docker.from_env()
            self.validate_yaml()
            if self.config['private_registry'] and self.config['username'] \
                    and self.config['password']:
                self.client.login(
                    registry=self.config['registry_url'],
                    username=self.config['username'],
                    password=self.config['password'])

    def validate_yaml(self):
        """Validate config yaml file
        """
        required_keys = [
            'airflow_cfg', 'private_registry', 'registry_url',
            'repository', 'image', 'tag', 'username', 'password', 'local_dir',
            'webserver_port', 'flower_port', 'rabbitmq', 'postgresql']
        for key in required_keys:
            assert key in self.config, (
                'key "{}" is not found in yaml.').format(key)
        required_mq_keys = [
            'name', 'username', 'password', 'host', 'virtual_host', 'image',
            'home', 'ui_port', 'port']
        for key in required_mq_keys:
            assert key in self.config['rabbitmq'], (
                'key "{}" is not found in yaml.').format(key)
        required_db_keys = [
            'name', 'username', 'password', 'host', 'image', 'data', 'port',
            'env']
        for key in required_db_keys:
            assert key in self.config['postgresql'], (
                'key "{}" is not found in yaml.'.format(key))

    def check_db_connection(self) -> bool:
        """Check if postgresql can be connected.
        Bubble up exception when fails.
        """
        try:
            airflow_cfg = self.config['airflow_cfg']
            db_string = airflow_cfg.get('AIRFLOW__CORE__SQL_ALCHEMY_CONN')
            result_backend = airflow_cfg.get('AIRFLOW__CELERY__RESULT_BACKEND')
            if not db_string or not result_backend:
                db_string = (
                    "postgresql+psycopg2://{}:{}@{}:{}/postgres").format(
                    self.config['postgresql']['username'],
                    self.config['postgresql']['password'],
                    self.config['postgresql']['host'],
                    self.config['postgresql']['port'])
            engine = create_engine(db_string)
            engine.table_names()
            return True
        except Exception as err:
            return False
        return False

    def check_rabbitmq_connection(self) -> bool:
        """Check Rabbitmq connection."""
        try:
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
        except Exception as err:
            return False
        return False

    def check_required_connections(self, funcs, echo=True) -> bool:
        """Check required connections.

        Args:
            funcs (list): list of references to check methods.
            echo (bool[optional]): True for printing out status.

        Returns:
            bool: True if all tests pass.
        """
        test_pass = True
        for func in funcs:
            if echo:
                print(
                    '{} result: {}'.format(
                        func.__name__,
                        'OK' if func() else 'FAILED!!'))
            test_pass &= func()
        return test_pass

    def pull(self):
        """Pull image"""
        self.client.images.pull(
            "{registry_url}/{repository}".format(
                registry_url=self.config['registry_url'],
                repository=self.config['repository']),
            tag=self.config['tag'])

    def build(self, dockerfile: str):
        """Build and push airflow docker image

        Args:
            dockerfile (str): full path to Dockerfile.
        """
        if not self.config['private_registry']:
            raise Exception(
                'Private registry flag is False. Please make sure your are '
                'building and pushing to private registry.')
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

    def exists(self, container_name: str) -> bool:
        """Check if container exists.

        Args:
            container_name (str): container name.
        """
        if container_name in [i.get('name') for i in self.list()]:
            return True
        return False

    def list(self) -> list:
        """List all containers

        Returns:
            list of contianers.
        """
        return [
            {"id": i.short_id, "name": i.name}
            for i in self.client.containers.list()]

    def kill(self, command="airflow_server"):
        """Kill container by name.

        Args:
            command (str): container name.
        """
        for container in self.client.containers.list():
            if container.name == command:
                container.kill()

    def _get_run_dict(self, name: str, command: list, ports=[], detach=True):
        """Get dictionary input for containers.run method.

        Args:
            name (str): name of container.
            command (list): list of string of commands.
            ports (list[optional]): list of int of port value.
            detach (bool[optional]): True for detaching container.
        """
        airflow_cfg = self.config['airflow_cfg']
        environment = [
            "{}={}".format(k, v) for k, v in airflow_cfg.items()
        ]
        result_backend = airflow_cfg.get('AIRFLOW__CELERY__RESULT_BACKEND')
        conn_str = airflow_cfg.get('AIRFLOW__CORE__SQL_ALCHEMY_CONN')
        if not conn_str or not result_backend:
            environment.append(
                ("AIRFLOW__CORE__SQL_ALCHEMY_CONN="
                 "postgresql+psycopg2://{}:{}@{}:{}/postgres").format(
                    self.config['postgresql']['username'],
                    self.config['postgresql']['password'],
                    self.config['postgresql']['host'],
                    self.config['postgresql']['port']
                ))
            environment.append(
                ("AIRFLOW__CELERY__RESULT_BACKEND="
                 "db+postgresql://{}:{}@{}:{}/postgres").format(
                    self.config['postgresql']['username'],
                    self.config['postgresql']['password'],
                    self.config['postgresql']['host'],
                    self.config['postgresql']['port']
                ))
        if 'AIRFLOW__CELERY__BROKER_URL' not in airflow_cfg.items():
            environment.append(
                ("AIRFLOW__CELERY__BROKER_URL="
                 "pyamqp://{}:{}@{}:{}/{}").format(
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
            environment=environment,
            volumes={
                '{}/dags'.format(self.config['local_dir']): {
                    'bind': airflow_cfg['AIRFLOW__CORE__DAGS_FOLDER'],
                    'mode': 'rw'
                },
                '{}/logs'.format(self.config['local_dir']): {
                    'bind': airflow_cfg['AIRFLOW__CORE__LOGS_FOLDER'],
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
        if self.exists(self.config['postgresql']['name']):
            print('Container {} already exists.'.format(
                self.config['postgresql']['name']
            ))
            return
        self.client.containers.run(
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
        if self.exists(self.config['rabbitmq']['name']):
            print('Container {} already exists.'.format(
                self.config['rabbitmq']['name']
            ))
            return
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
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers))
        assert self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection]), (
            'Required connections not satisfied.')
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
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers))
        assert self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection]), (
            'Required connections not satisfied.')
        return self.client.containers.run(
            **self._get_run_dict(name, ["scheduler"], detach=detach))

    def start_worker(self, queue, name='airflow_worker', detach=True):
        """Docker run airflow worker.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers))
        assert self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection]), (
            'Required connections not satisfied.')
        return self.client.containers.run(
            **self._get_run_dict(
                name, ["worker", "-q", queue], [8793], detach=True))

    def start_flower(self, name='airflow_flower', detach=True):
        """Docker run airflow worker.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers))
        assert self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection]), (
            'Required connections not satisfied.')
        return self.client.containers.run(
            **self._get_run_dict(name, [
                "flower", "-p", str(self.config['flower_port'])
            ], [self.config['flower_port']], detach=True))

    def start_initdb(self, detach=True, echo=True):
        """Docker run airflow initdb
        Args:
            detach (bool[optional]): True for detach container.
            echo (bool[optional]): True for printing out status.
        """
        assert self.check_required_connections(
            [self.check_db_connection], echo), (
            'Required connections not satisfied.')
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
        '--kill', dest='kill', action='store_true',
        help='Kill running services.')
    parser.add_argument(
        '--config', dest='config',
        help='Specify path to config yaml file.')
    parser.add_argument(
        '--dockerfile', dest='dockerfile', help='Path to the Dockerfile.')
    parser.add_argument(
        '--run', dest='run',
        help=(
            'command name: (webserver, rabbitmq, scheduler, worker, flower, '
            'initdb, postgresql)'))
    parser.add_argument(
        '--queue', dest='queue',
        help='Queue name for the worker.')
    parser.set_defaults(queue='default')
    parser.set_defaults(config='./config.yaml')
    parser.set_defaults(dockerfile='./Dockerfile')
    args = parser.parse_args()

    if not args.build and not args.run and not args.list and not args.kill \
            and not args.pull:
        parser.print_help()
    if args.build:
        a = AirflowRun(args.config)
        if not os.path.exists(args.config):
            raise Exception('--config path to config file is invalid.')
        if not args.dockerfile or not os.path.exists(args.dockerfile):
            raise Exception('--dockerfile path to Dockerfile is invalid.')
        a.build(os.path.dirname(args.dockerfile))
    elif args.list:
        a = AirflowRun(args.config)
        for i in a.list():
            print('id: {} name: {}'.format(i['id'], i['name']))
    elif args.kill:
        a = AirflowRun(args.config)
        running_services = a.list()
        if len(running_services) > 0:
            print('\nContainers:')
            print('-----------')
            for index, i in enumerate(running_services):
                print('{}. {}'.format(index, i['name']))
            print('a. Kill all.')
            print('c. Cancel.')
            choice = input('Choose one: ')
            if choice == 'a':
                for i in running_services:
                    a.kill(i['name'])
            elif choice == 'c':
                return
            else:
                a.kill(running_services[int(choice)]['name'])
        else:
            print('No running service found.')
    elif args.run:
        a = AirflowRun(args.config)
        a.client.containers.prune()
        a.pull()
        if args.run == "worker":
            a.start_worker(args.queue)
        elif args.run == "postgresql":
            a.start_postgresql()
            choice = input('Run initdb? (y/n): ') or 'n'
            if choice.lower() == 'y':
                successful = False
                print('Running initdb...')
                timeout = 30
                while not successful and timeout > 0:
                    try:
                        self.start_initdb(echo=False)
                        successful = True
                    except Exception:
                        time.sleep(1)
                        timeout -= 1
        elif args.run in a.supported_services:
            getattr(a, 'start_{}'.format(args.run))()
        else:
            print('\nAvailable services:')
            print('-------------------')
            for index, i in enumerate(a.supported_services):
                print("{}. {}".format(index, i))

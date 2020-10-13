import argparse
import docker
import pika
import os
import socket
from sqlalchemy import create_engine
import time
import yaml

from airflow_run.decorators import retry
from airflow_run.utils import logger_factory
from cryptography.fernet import Fernet


class AirflowRun(object):
    def __init__(self, config: str, log: bool = False):
        """Constructor

        Args:
            config (str): path to config.yaml file.
            log (boolean): True for showing logs.
        """

        try:
            self._hostname = socket.gethostname()
            self._ip = socket.gethostbyname(self._hostname)
        except Exception as err:
            self._hostname = 'localhost'
            self._ip = socket.gethostbyname(self._hostname)
        self._show_log = log
        self._logger = logger_factory()
        self.supported_services = [
            'flower', 'initdb', 'postgresql', 'postgres', 'rabbitmq',
            'scheduler', 'webserver', 'worker', 'list', 'airflow_scheduler',
            'airflow_webserver', 'airflow_worker', 'all']
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
            'env', 'private_registry', 'registry_url',
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

    @retry(7, 'Checking Postgresql connection...')
    def check_db_connection(self) -> bool:
        """Check if postgresql can be connected.
        Bubble up exception when fails.
        """
        if self._show_log:
            self._logger.info('Checking DB connection...')
        env = self.config['env']
        db_string = env.get('AIRFLOW__CORE__SQL_ALCHEMY_CONN')
        result_backend = env.get(
            'AIRFLOW__CELERY__RESULT_BACKEND')
        if not db_string or not result_backend:
            db_string = (
                "postgresql+psycopg2://{}:{}@{}:{}/postgres").format(
                self.config['postgresql']['username'],
                self.config['postgresql']['password'],
                self.config['postgresql']['host'],
                self.config['postgresql']['port'])
        engine = create_engine(db_string, connect_args={'connect_timeout': 20})
        engine.table_names()
        self._logger.debug('Database connection is: OK')
        return True

    @retry(7, 'Checking Rabbitmq connection...')
    def check_rabbitmq_connection(self) -> bool:
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
            self._logger.debug('Rabbitmq connection is: OK')
            connection.close()
            return True
        raise Exception('Fail to connect to Rabbitmq.')

    def check_required_connections(self, funcs) -> bool:
        """Check required connections.

        Args:
            funcs (list): list of references to check methods.
            echo (bool[optional]): True for printing out status.

        Returns:
            bool: True if all tests pass.
        """
        test_pass = True
        for func in funcs:
            test_pass &= func()
        return test_pass

    def pull(self):
        """Pull image"""
        os.system('docker pull {image}:{tag}'.format(
            image="{registry_url}/{repository}".format(
                registry_url=self.config['registry_url'],
                repository=self.config['repository']),
            tag=self.config['tag']))

    def build(self, dockerfile: str):
        """Build and push airflow docker image

        Args:
            dockerfile (str): full path to Dockerfile.
        """
        if not self.config['private_registry']:
            raise Exception(
                'Private registry flag is False. Please make sure your are '
                'building and pushing to private registry.')
        self._logger.debug(self.client.images.build(
            path=os.path.realpath(dockerfile),
            buildargs=self.config['env'],
            tag=self.config['tag']))
        image = self.client.images.get(self.config['image'])
        image.tag(
            repository='{}/{}'.format(
                self.config['registry_url'], self.config['repository']),
            tag=self.config['tag'])
        self._logger.debug(self.client.images.push(
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
            for i in self.client.containers.list()
            if i.name in self.supported_services]

    def kill(self, command):
        """Kill container by name.

        Args:
            command (str): container name.
        """
        for container in self.client.containers.list():
            if container.name == command:
                container.kill()

    def _get_environment_variables(self):
        """Get airflow environment variables.

        Returns:
            list: list of environment variables to be passed to docker run.
        """
        env = self.config['env']
        environment = [
            "{}={}".format(k, v) for k, v in env.items()
        ]
        result_backend = env.get('AIRFLOW__CELERY__RESULT_BACKEND')
        conn_str = env.get('AIRFLOW__CORE__SQL_ALCHEMY_CONN')
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
        if 'AIRFLOW__CELERY__BROKER_URL' not in env.items():
            environment.append(
                ("AIRFLOW__CELERY__BROKER_URL="
                 "pyamqp://{}:{}@{}:{}/{}").format(
                    self.config['rabbitmq']['username'],
                    self.config['rabbitmq']['password'],
                    self.config['rabbitmq']['host'],
                    self.config['rabbitmq']['port'],
                    self.config['rabbitmq']['virtual_host']
                ))
        return environment

    def get_docker_run_command(self, bash_command: list, ports: list = []):
        env = self.config['env']
        command = (
            'docker run -d {env} {volumes} {ports} {registry_url}/{repository}:{tag} {bash_command}').format(
            env=' '.join(['--env {}'.format(i)
                          for i in self._get_environment_variables()]),
            volumes=' '.join(['-v {}:{}'.format(i['host_path'], i['container_path'])
                              for i in self.config['custom_mount_volumes']]),
            ports=' '.join(['-p {}:{}'.format(p[0], p[1]) for p in ports]),
            image=self.config['image'],
            registry_url=self.config['registry_url'],
            repository=self.config['repository'],
            tag=self.config['tag'],
            bash_command=' '.join(bash_command))
        self._logger.debug('Running command: \n\n{}\n\n'.format(command))
        return command

    def _get_run_dict(self, name: str, command: list, ports=[], detach=True):
        """Get dictionary input for containers.run method.

        Args:
            name (str): name of container.
            command (list): list of string of commands.
            ports (list[optional]): list of int of port value.
            detach (bool[optional]): True for detaching container.
        """

        env = self.config['env']
        volumes = {
            '{}/dags'.format(self.config['local_dir']): {
                'bind': env['AIRFLOW__CORE__DAGS_FOLDER'],
                'mode': 'rw'
            },
            '{}/logs'.format(self.config['local_dir']): {
                'bind': env['AIRFLOW__CORE__BASE_LOG_FOLDER'],
                'mode': 'rw'
            }
        }
        if 'custom_mount_volumes' in self.config and len(
                self.config['custom_mount_volumes']) > 0:
            for custom_mount_volume in self.config['custom_mount_volumes']:
                assert 'host_path' in custom_mount_volume
                assert 'container_path' in custom_mount_volume
                volumes_to_be_deleted = []
                for k, v in volumes.items():
                    if v['bind'] == custom_mount_volume['container_path']:
                        volumes_to_be_deleted.append(k)
                for volume_to_be_deleted in volumes_to_be_deleted:
                    del volumes[volume_to_be_deleted]
                volumes[custom_mount_volume['host_path']] = {
                    'bind': custom_mount_volume['container_path'],
                    'mode': 'rw'
                }
        output = dict(
            image='{registry_url}/{repository}:{tag}'.format(
                registry_url=self.config['registry_url'],
                repository=self.config['repository'],
                tag=self.config['tag']),
            name=name,
            auto_remove=True,
            detach=detach,
            environment=self._get_environment_variables(),
            volumes=volumes,
            command=command)
        if ports:
            ports_dic = {}
            for p in ports:
                if type(p) == int:
                    ports_dic['{}/tcp'.format(p)] = p
                else:
                    ports_dic['{}/tcp'.format(p.split(':')[0])
                              ] = p.split(':')[1]
            output.update(ports=ports_dic)
        return output

    def start_postgresql(self):
        """Start postgres instance.
        """
        os.system('docker pull {image}:{tag}'.format(
            image=self.config['postgresql']['image'],
            tag=self.config['postgresql']['tag']))
        if self.exists(self.config['postgresql']['name']):
            self._logger.debug('Container {} already exists.'.format(
                self.config['postgresql']['name']
            ))
            return
        self._logger.info('Starting postgres...')
        command = ('docker run -d {port} {env} {volumes} {image}:{tag}').format(
            port='-p {port}:{port}'.format(
                port=self.config['postgresql']['port']),
            env=' '.join(
                ['--env {}={}'.format(k, v)
                 for k, v in self.config['postgresql']['env'].items()]),
            volumes='-v {}/postgresql:{}'.format(
                    self.config['local_dir'],
                    self.config['postgresql']['data']),
            image=self.config['postgresql']['image'],
            tag=self.config['postgresql']['tag'])
        self._logger.debug('Running command: \n\n{}\n\n'.format(command))
        os.system(command)

    def start_postgres(self):
        return self.start_postgresql()

    def start_rabbitmq(self):
        """Docker run rabbitmq default image."""
        if self.exists(self.config['rabbitmq']['name']):
            self._logger.debug('Container {} already exists.'.format(
                self.config['rabbitmq']['name']
            ))
            return
        self._logger.info('Starting rabbitmq...')
        command = ('docker run -d {env} {volumes} {ports} {image}'.format(
            env=(
                ' '.join(['-e {}={}'.format(k, v)
                          for k, v in self.config['rabbitmq']['env'].items()])),
            volumes=(
                '-v {}={}'.format(
                    self.config['local_dir'],
                    self.config['rabbitmq']['home'])),
            ports='-p {ui_port}:{ui_port} -p {port}:{port}'.format(
                ui_port=self.config['rabbitmq']['ui_port'],
                port=self.config['rabbitmq']['port']),
            image=self.config['rabbitmq']['image']
        ))
        self._logger.debug('Running command: \n\n{}\n\n'.format(command))
        os.system(command)
        self._logger.info(
            'Rabbitmq UI url: {ip}:{port}'.format(
                ip=self._ip, port=self.config['rabbitmq']['ui_port']))

    def start_webserver(self, name='airflow_webserver', detach=True):
        """Docker run airflow webserver.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        self._logger.info('Starting webserver...')
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers))
        self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection])
        webserver_port = str(self.config['webserver_port'])
        command = self.get_docker_run_command(
            ["webserver", "-p", webserver_port],
            ports=[(webserver_port, webserver_port)])
        os.system(command)
        self._logger.info(
            'Webserver url: {ip}:{port}'.format(
                ip=self._ip, port=self.config['webserver_port']))

    def start_airflow_webserver(self, **kwargs):
        self.start_webserver(**kwargs)

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
        self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection])
        command = self.get_docker_run_command(["scheduler"])
        os.system(command)

    def start_airflow_scheduler(self, **kwargs):
        return self.start_scheduler(**kwargs)

    def start_worker(
            self, queue, worker_log_server_port=8793, name='airflow_worker',
            detach=True):
        """Docker run airflow worker.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
            worker_log_server_port (int|str[optional]): worker log server port.
                if str, format is: "inbound port:outbound port"
        """
        self._logger.info('Starting worker with queue: {}...'.format(queue))
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers) + 1)
        self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection])
        outbound_port = worker_log_server_port + len(running_workers)
        command = self.get_docker_run_command(
            ["worker", "-q", queue],
            ports=[(worker_log_server_port, outbound_port)])
        os.system(command)

    def start_airflow_worker(self, **kwargs):
        return self.start_worker(**kwargs)

    def start_flower(self, name='airflow_flower', detach=True):
        """Docker run airflow worker.
        Args:
            name (str): name of the container.
            detach (bool[optional]): True for detach container.
        """
        self._logger.info('Starting flower...')
        running_workers = [
            i.get('name') for i in self.list() if name in i['name']]
        if len(running_workers) > 0:
            name += '_{}'.format(len(running_workers))
        self.check_required_connections(
            [self.check_db_connection, self.check_rabbitmq_connection])
        flower_port = str(self.config['flower_port'])
        command = self.get_docker_run_command(
            ["flower", "-p", flower_port],
            ports=[(flower_port, flower_port)])
        os.system(command)
        self._logger.info(
            'Flower url: {ip}:{port}'.format(
                ip=self._ip, port=self.config['flower_port']))

    def start_initdb(self, detach=False):
        """Docker run airflow initdb
        Args:
            detach (bool[optional]): True for detach container.
            echo (bool[optional]): True for printing out status.
        """

        self._logger.info('Running airflow initdb...')
        self.check_required_connections([self.check_db_connection])
        self.client.containers.prune()
        command = self.get_docker_run_command(["initdb"])
        os.system(command)

    @staticmethod
    def generate_config():
        """Generate config yaml file
        """
        path = os.path.join(os.path.dirname(__file__), 'config-template.yaml')
        with open(path, 'r') as fr:
            content = yaml.safe_load(fr)
            local_dir = input(
                "Please enter local path which contains /dags and /logs: ")
            rabbitmq_host = input("Please enter rabbitmq host/ip: ")
            rabbitmq_username = input("Please enter rabbitmq username: ")
            rabbitmq_password = input("Please enter rabbitmq password: ")
            postgresql_host = input("Please enter postgresql host/ip: ")
            postgresql_username = input("Please enter postgresql username: ")
            postgresql_password = input("Please enter postgresql password: ")
            content['env']['AIRFLOW__CORE__FERNET_KEY'] = Fernet.generate_key().decode()
            content['local_dir'] = local_dir
            content['rabbitmq']['host'] = rabbitmq_host
            content['rabbitmq']['username'] = rabbitmq_username
            content['rabbitmq']['password'] = rabbitmq_password
            content['rabbitmq']['env']['RABBITMQ_DEFAULT_USER'] = rabbitmq_username
            content['rabbitmq']['env']['RABBITMQ_DEFAULT_PASS'] = rabbitmq_password
            content['postgresql']['host'] = postgresql_host
            content['postgresql']['username'] = postgresql_username
            content['postgresql']['password'] = postgresql_password
            content['postgresql']['env']['POSTGRES_USER'] = postgresql_username
            content['postgresql']['env']['POSTGRES_PASSWORD'] = postgresql_password
        with open('config.yaml', 'w') as fw:
            yaml.dump(content, fw, default_flow_style=False,
                      sort_keys=False)
        print('Created file: {}'.format(os.path.realpath('config.yaml')))


def cli():
    parser = argparse.ArgumentParser(description='Airflow Run')
    parser.add_argument(
        '--generate_config', dest='generate_config',
        action='store_true', help='Generate config file.')
    parser.add_argument(
        '--build', dest='build', action='store_true',
        help='Path to the Dockerfile.')
    parser.add_argument(
        '--pull', dest='pull', action='store_true',
        help='Pull latest image.')
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
        '--log', dest='log', action='store_true', help='show logs')
    parser.add_argument(
        '--run', dest='run',
        help=(
            'command name: (webserver, rabbitmq, scheduler, worker, flower, '
            'initdb, postgresql, list)'))
    parser.add_argument(
        '--queue', dest='queue',
        help='Queue name for the worker.')
    parser.add_argument(
        '--worker_log_server_port', dest='worker_log_server_port',
        help='worker_log_server_port for worker')
    parser.set_defaults(queue='default')
    parser.set_defaults(config='./config.yaml')
    if os.getenv('AIRFLOWRUN_CONFIG_PATH'):
        parser.set_defaults(config=os.getenv('AIRFLOWRUN_CONFIG_PATH'))
    parser.set_defaults(dockerfile='./Dockerfile')
    parser.set_defaults(worker_log_server_port=8793)
    parser.set_defaults(log=False)
    args = parser.parse_args()

    if not args.build and not args.run and not args.list and not args.kill and not args.pull and not args.generate_config and not args.log:
        parser.print_help()

    if args.build:
        airflow_run = AirflowRun(args.config, log=args.log)
        if not os.path.exists(args.config):
            raise Exception('--config path to config file is invalid.')
        if not args.dockerfile or not os.path.exists(args.dockerfile):
            raise Exception('--dockerfile path to Dockerfile is invalid.')
        airflow_run.build(os.path.dirname(args.dockerfile))
    elif args.generate_config:
        AirflowRun.generate_config()
    elif args.list:
        airflow_run = AirflowRun(args.config, log=args.log)
        for i in airflow_run.list():
            print('id: {} name: {}'.format(i['id'], i['name']))
    elif args.kill:
        airflow_run = AirflowRun(args.config, log=args.log)
        running_services = airflow_run.list()
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
                    airflow_run.kill(i['name'])
            elif choice == 'c':
                return
            else:
                airflow_run.kill(running_services[int(choice)]['name'])
        else:
            print('No running service found.')
    elif args.run:
        airflow_run = AirflowRun(args.config, log=args.log)
        airflow_run.client.containers.prune()
        airflow_run.pull()
        if args.run == "worker":
            airflow_run.start_initdb()
            airflow_run.start_worker(
                queue=args.queue,
                worker_log_server_port=args.worker_log_server_port)
        elif args.run in ["postgresql", "postgres"]:
            airflow_run.start_postgresql()
            airflow_run.start_initdb()
        elif args.run == "rabbitmq":
            airflow_run.start_rabbitmq()
            airflow_run.start_initdb()
        elif args.run == "webserver":
            airflow_run.start_webserver()
            airflow_run.start_initdb()
        elif args.run == "scheduler":
            airflow_run.start_scheduler()
            airflow_run.start_initdb()
        elif args.run == "flower":
            airflow_run.start_flower()
        elif args.run == "all":
            airflow_run.start_postgresql()
            airflow_run.start_initdb()
            airflow_run.start_rabbitmq()
            airflow_run.start_initdb()
            airflow_run.start_scheduler()
            airflow_run.start_worker(
                queue=args.queue,
                worker_log_server_port=args.worker_log_server_port)
            airflow_run.start_webserver()
        else:
            print('\nAvailable services:')
            print('-------------------')
            for index, i in enumerate(airflow_run.supported_services):
                print("{}. {}".format(index, i))

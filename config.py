import os
import time
from configparser import ConfigParser
from typing import Dict, Any, Tuple, Callable

import psycopg2
import pandas as pd
from psycopg2 import sql
from loguru import logger
from sshtunnel import SSHTunnelForwarder

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', 20)


def read_config(file_name: str = 'creds//db.ini', section: str = 'postgresql') -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Read configuration values from a specified file.
    :param file_name: name of the configuration file (default: 'creds//db.ini').
    :param section: section name in the configuration file (default: 'postgresql').
    :return: A tuple containing two dictionaries: ssh_param and db_param.
       ssh_param: dict containing SSH connection parameters.
       db_param: dict containing database connection parameters.
    """
    config = ConfigParser()
    config.read(file_name)
    ssh_param = {
        'ssh_host': config.get(section, 'ssh_host').split(':')[0],
        'ssh_port': int(config.get(section, 'ssh_host').split(':')[1]),
        'ssh_private_key': os.path.expanduser(config.get(section, 'ssh_private_key_path')),
        'ssh_username': config.get(section, 'ssh_username'),
        'remote_bind_address': (
            config.get(section, 'remote_bind_address').split(':')[0],
            int(config.get(section, 'remote_bind_address').split(':')[1])
        ),
    }
    db_param = {
        'dbname': config.get(section, 'db_name'),
        'user': config.get(section, 'db_user'),
        'password': config.get(section, 'db_password'),
        'host': config.get(section, 'db_host'),
    }
    return ssh_param, db_param


def create_ssh_tunnel(params: Dict[str, Any]) -> SSHTunnelForwarder:
    """
    Create an SSH tunnel using the provided parameters.

    :param params: dict containing SSH tunnel parameters.
    :return: instance of SSHTunnelForwarder.
    """
    return SSHTunnelForwarder(**params)


def connect_to_database(params: Dict[str, Any], port: int):
    """
    Connect to a database using the provided parameters and port.

    :param params: dict containing database connection parameters.
    :param port: port number for the database connection.
    :return: database connection object.
    """
    return psycopg2.connect(port=port, **params)


def execute_sql_query(connection, sql_query) -> pd.DataFrame:
    """
    Execute an SQL query on the provided database connection and return the result as a df.

    :param connection: db connection object.
    :param sql_query: SQL query to be executed.
    :return: result of the SQL query as a DataFrame.
    """
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchall()
        return pd.DataFrame(result, columns=columns)


def timed(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator that measures and logs the execution time of a function.

    :param func: function to be decorated.
    :return: decorated function.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.time()
        result = func(*args, **kwargs)
        duration = "[{name}]: Time spend: {elapsed:.2f}s".format(
            name=func.__name__.upper(),
            elapsed=time.time() - start
        )
        logger.success(duration)
        return result

    return wrapper


@timed
def get_data_from_db(query_str: str) -> pd.DataFrame:
    """
    Get data from the database using the provided SQL query.

    :param query_str: SQL query string.
    :return: queried data as a DataFrame.
    """
    ssh_params, db_params = read_config()
    with create_ssh_tunnel(ssh_params) as server:
        db_port = server.local_bind_port
        conn = connect_to_database(db_params, db_port)
        query = sql.SQL(query_str)
        result = execute_sql_query(conn, query)
    return pd.DataFrame(result)

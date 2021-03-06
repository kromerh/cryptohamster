from datetime import datetime
from typing import Tuple, Union
import pandas as pd
import pymysql
import os

from constants import (
    FULL_PATH_TO_CREDENTIALS,
    HOST,
    DATABASE,
    PORT
)


def log(log_path: str, logmsg: str, printout: bool = False) -> None:
    """Function to add a line to a logfile.

    Args:
        log_path: Full path to the filename with the log.
        logmsg: Message to be appended to the log file.
        printout: If True, `logmsg` is also printed out. Defaults to False.

    Returns:
        None.
    """
    # Add the current timestamp to the log
    logmsg = f'{datetime.now()} - {logmsg}'

    if printout:
        print(logmsg)

    with open(log_path, 'a') as f:
        f.write('\n')
        f.write(logmsg)
        f.close()


def load_credentials(filepath: str) -> Tuple[str, str]:
    """Function to load credentials from a file.

    Args:
        filepath: Full file path to the credentials file.
    
    Returns:
        (user, password): Tuple of user and password strings.
    """
    with open(filepath) as f:
        lines = f.readlines()
        f.close()
    user = lines[0].split(':')[1].strip()
    password = lines[1].split(':')[1].strip()

    return (user, password)

def get_latest_row_by_id(
    mysql_connection: pymysql.connections.Connection,
    table: str,
    id_col: str,
    num_rows: int = 1
    ) -> Union[None, pd.core.series.Series, pd.DataFrame]:
    """Function to get the latest row of a table by id.

    The table is ordered descending by the `id_col` the last row is returned.

    Args:
        mysql_connection: MySQL connection
        table: Table name.
        id_col: ID column. 
    
    Returns:
        Either None or the past num_rows as series (num_rows = 1) or dataframe (num_rows > 1). 
    """
    qry = f'SELECT * FROM {table} ORDER BY {id_col} DESC LIMIT {num_rows}'
    df = pd.read_sql(
        sql=qry,
        con=mysql_connection,
        index_col=id_col
    )
    if len(df) > 0:
        if num_rows == 1:
            df = df.iloc[-1, :]
            return df
        else:
            return df
    else:
        return None

def create_mysql_connection(
    full_path_to_credentials: str = FULL_PATH_TO_CREDENTIALS,
    host: str = HOST,
    db: str = DATABASE,
    port: int = PORT
    ) -> pymysql.connections.Connection:
    """Function to connect to the database.

    Args:
        full_path_to_credentials: Full path to the credentials to connect to the database.
        host: Hostname.
        db: Database name.
        port: Which port to use.
    """
    # Load credentials to connect to the database
    # cwd = os.getcwd()
    user, password = load_credentials(filepath=full_path_to_credentials)

    # Create mysql connection
    mysql_connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8'
    )

    return mysql_connection

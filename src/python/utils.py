from datetime import datetime
from typing import Tuple
import pandas as pd
import pymysql

from constants import DB_TBL

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


def is_magnet_closed(
    mysql_connection: pymysql.connections.Connection,
    threshold: float = 0.725
    ) -> bool:
    """Function that returns True if the magnet is stuck in closed position.

    When the wheel stops just when the magnet touches the sensor, there would be 
    infinite readings into the database, we want to avoid this!

    Args:
        mysql_connection: MySQL connection
        threshold: Threshold in seconds above which the average must lie.

    Returns:
        True if average of the last 20 readings is smaller than 0.75 seconds, else False.
    """
    # Get the past 20 readings of the hamsterwheel
    query = f"SELECT * FROM {DB_TBL['HAMSTERWHEEL']['name']} ORDER BY time DESC LIMIT 20"
    df = pd.read_sql(
        sql=query,
        con=mysql_connection,
        index_col='hamsterwheel_id'
    )
    # Compute average
    avg_time_diff = (df['time'].shift(1) - df['time']).dt.total_seconds().values[1:].mean()
    # Check if average below threshold
    if avg_time_diff < threshold:
        return True
    else:
        return False


def get_latest_row_by_id(
    mysql_connection: pymysql.connections.Connection,
    table: str,
    id_col: str
    ) -> pd.core.series.Series:
    """Function to get the latest row of a table by id.

    The table is ordered descending by the `id_col` the last row is returned.

    Args:
        mysql_connection: MySQL connection
        table: Table name.
        id_col: ID column. 
    """
    qry = f'SELECT * FROM {table} ORDER BY {id_col} DESC LIMIT 1'
    df = pd.read_sql(
        sql=qry,
        con=mysql_connection,
        index_col=id_col
    )
    df = df.iloc[-1, :]

    return df


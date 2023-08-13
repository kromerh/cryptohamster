"""
In this script the raw_hamsterwheel table is transformed into the closed_hamsterwheel table.
"""

import pymysql
import pandas as pd
import logging
from logger import remove_rows

logging.basicConfig(
    filename=f"/home/done4/log_logger.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

logging.info("Running Logger")

logger = logging.getLogger()


def read_last_mins_from_table(
    mysql_connection: pymysql.connections.Connection, table: str, minutes: int
) -> pd.DataFrame:
    qry = f"SELECT * FROM {table} WHERE time > NOW() - INTERVAL {minutes} MINUTE;"
    df = pd.read_sql(sql=qry, con=mysql_connection, index_col="id")
    df["time"] = df["time"].astype(str)
    return df


def filter_on_closed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter on the closed hamsterwheel
    """
    df = df[df["magnet"] == 0]
    return df


def update_closed_hamsterwheel(
    cursor: pymysql.cursors.Cursor,
    mysql_connection: pymysql.connections.Connection,
    df: pd.DataFrame,
) -> None:
    for index, row in df.iterrows():
        time = row["time"]
        magnet = row["magnet"]
        hash = row["hash"]
        query = f"INSERT INTO closed_hamsterwheel (hash, time, magnet) VALUES ('{hash}', '{time}', {magnet})"
        cursor.execute(query)
        mysql_connection.commit()


def delete_rows_raw_hamsterwheel(
    cursor: pymysql.cursors.Cursor,
    minutes: int,
) -> None:
    query = (
        f"DELETE FROM raw_hamsterwheel WHERE time < NOW() - INTERVAL {minutes} MINUTE;"
    )
    cursor.execute(query)


if __name__ == "__main__":
    try:
        mysql_connection = pymysql.connect(
            host="192.168.1.121",
            user="tiberius",
            password="q123",
            db="cryptohamster",
            port=3306,
            charset="utf8",
        )
        # Read everything from closed_hamsterwheel from last 30 min
        df_closed_hamsterwheel = read_last_mins_from_table(
            mysql_connection=mysql_connection, table="closed_hamsterwheel", minutes=30
        )
        logger.info(
            f"Read {len(df_closed_hamsterwheel)} rows from closed_hamsterwheel."
        )
        # Filter on closed
        df_closed_hamsterwheel = filter_on_closed(df=df_closed_hamsterwheel)
        # Read everything from raw_hamsterwheel from last 2 min
        df_raw_hamsterwheel = read_last_mins_from_table(
            mysql_connection=mysql_connection, table="raw_hamsterwheel", minutes=2
        )
        logger.info(f"Read {len(df_raw_hamsterwheel)} rows from raw_hamsterwheel.")
        # Remove rows from raw_hamsterwheel that are already in closed_hamsterwheel
        df_raw_hamsterwheel = remove_rows(
            df_1=df_raw_hamsterwheel,
            df_2=df_closed_hamsterwheel,
            compare_col="hash",
        )
        # Update closed_hamsterwheel
        update_closed_hamsterwheel(
            cursor=mysql_connection.cursor(),
            mysql_connection=mysql_connection,
            df=df_raw_hamsterwheel,
        )
        logger.info(
            f"Closed hamsterwheel updated, added {len(df_raw_hamsterwheel)} rows."
        )
        # Delete rows from raw_hamsterwheel
        delete_rows_raw_hamsterwheel(
            cursor=mysql_connection.cursor(),
            minutes=3,
        )

        mysql_connection.close()
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")
        mysql_connection.close()

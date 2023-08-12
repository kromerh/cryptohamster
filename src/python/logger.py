import time
from typing import Generator, List
import re
import pymysql
import sys
import pandas as pd
import warnings
from datetime import datetime
import hashlib
import logging

logging.basicConfig(
    filename=f"/home/done4/log_logger.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

logging.info("Running Loggern")

logger = logging.getLogger()

warnings.simplefilter(action="ignore", category=UserWarning)
# def follow(thefile: str) -> Generator[str, None, None]:
#     thefile.seek(0, 2)  # Go to the end of the file
#     while True:
#         line = thefile.readline()
#         if not line:
#             time.sleep(0.1)  # Sleep briefly
#             continue
#         yield line


def read_last_n_lines(file_name: str, n: int) -> list:
    with open(file_name, "r") as f:
        lines = f.readlines()
        last_n_lines = lines[-n:]
        return last_n_lines


def extract_date(line: str) -> str:
    return line.split(" - ")[0]


def extract_magnet(line: str) -> str:
    return line.split(" - ")[1]


def check_date(date: str) -> bool:
    if not re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", date):
        return False
    return True


def check_line(line: str) -> bool:
    if " - " not in line:
        return False

    # Check the date is like '2023-08-08 13:59:59.763742'
    date = extract_date(line)
    if not re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+", date):
        return False

    # Check that magnet is either 0 or 1
    magnet = extract_magnet(line)
    if not re.match(r"0|1", magnet):
        return False

    return True


def update_raw_hamsterwheel(
    cursor: pymysql.cursors.Cursor,
    mysql_connection: pymysql.connections.Connection,
    df: pd.DataFrame,
) -> None:
    for index, row in df.iterrows():
        time = row["time"]
        magnet = row["magnet"]
        hash = row["hash"]
        query = f"INSERT INTO raw_hamsterwheel (hash, time, magnet) VALUES ('{hash}', '{time}', {magnet})"
        cursor.execute(query)
        mysql_connection.commit()


def read_last_n_rows_from_raw_hamsterwheel(
    mysql_connection: pymysql.connections.Connection, n: int
) -> pd.DataFrame:
    qry = f"SELECT * FROM raw_hamsterwheel ORDER BY id DESC LIMIT {n}"
    df = pd.read_sql(sql=qry, con=mysql_connection, index_col="id")
    df["time"] = df["time"].astype(str)
    return df


def create_df_from_log(dates: List[str], magnets: List[str]) -> pd.DataFrame:
    df = pd.DataFrame(
        data={
            "time": dates,
            "magnet": magnets,
        }
    )
    return df


def remove_rows(
    df_1: pd.DataFrame, df_2: pd.DataFrame, compare_col: str
) -> pd.DataFrame:
    """Remove rows in df_1 that are already in df_2 based on compare_cols."""
    filter_rows = df_1[compare_col].isin(df_2[compare_col])
    df = df_1[~filter_rows]
    return df


def get_filename() -> str:
    """Get filename from now."""
    now = datetime.now()
    time_prefix = f"{now.year}{now.month}{now.day}{now.hour}_"
    log_path_fname = f"/home/done4/logs/{time_prefix}log.log"
    return log_path_fname


def add_hash_column(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Add hash column to df based on columns."""
    df["hash"] = (
        df[columns]
        .astype(str)
        .apply(lambda x: hashlib.md5(x.to_string().encode()).hexdigest(), axis=1)
    )
    return df


if __name__ == "__main__":
    while True:
        try:
            file_name = get_filename()
            # Create mysql connection
            mysql_connection = pymysql.connect(
                host="192.168.1.121",
                user="tiberius",
                password="q123",
                db="cryptohamster",
                port=3306,
                charset="utf8",
            )

            # Get Cursor
            cursor = mysql_connection.cursor()
            loglines = read_last_n_lines(file_name=file_name, n=20)
            dates = []
            magnets = []
            for line in loglines:
                if check_line(line):
                    line = line.strip()
                    date = extract_date(line)
                    magnet = extract_magnet(line)
                    dates.append(date)
                    magnets.append(magnet)

            df_new = create_df_from_log(dates=dates, magnets=magnets)
            df_new = add_hash_column(df=df_new, columns=["time", "magnet"])
            df_read = read_last_n_rows_from_raw_hamsterwheel(
                mysql_connection=mysql_connection, n=20
            )
            df = remove_rows(df_1=df_new, df_2=df_read, compare_col="hash")
            if len(df) > 0:
                logger.info(f"Saving to DB new rows: {len(df)}")
                update_raw_hamsterwheel(
                    cursor=cursor, mysql_connection=mysql_connection, df=df
                )
            mysql_connection.close()
            time.sleep(1)
        except KeyboardInterrupt:
            print("Exiting")
            mysql_connection.close()
            sys.exit(0)
        except Exception as e:
            logger.error(f"Error: {e}")
            mysql_connection.close()
            continue

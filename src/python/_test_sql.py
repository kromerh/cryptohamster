"""
In this script the raw_hamsterwheel table is transformed into the closed_hamsterwheel table.
"""

import pymysql
import pandas as pd
import logging

logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

logging.info("Running Logger")

logger = logging.getLogger()


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
        # Delete rows from raw_hamsterwheel
        delete_rows_raw_hamsterwheel(
            cursor=mysql_connection.cursor(),
            minutes=3,
        )
        mysql_connection.commit()
        mysql_connection.close()
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")
        mysql_connection.close()

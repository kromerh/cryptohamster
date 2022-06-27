# --------------------------------------------------------
# Script to simulate the hamster running, uses TEST tables
# --------------------------------------------------------

import pymysql
import sys
import time

from utils import (
    create_mysql_connection,
)

from constants import (
    FULL_PATH_TO_CREDENTIALS,
    DB_TBL,
)

mysql_connection = create_mysql_connection(full_path_to_credentials=FULL_PATH_TO_CREDENTIALS)


# Get Cursor
cursor = mysql_connection.cursor()

try:
    while True:
        print('Inserting...' + DB_TBL['HAMSTERWHEEL']['name'])
        cursor.execute(f"INSERT INTO {DB_TBL['HAMSTERWHEEL']['name']} (flag) VALUES (True)")
        mysql_connection.commit()
        time.sleep(1)
except KeyboardInterrupt:
    mysql_connection.close()

import RPi.GPIO as io
import mariadb
import sys
import time

from utils import (
    load_credentials,
    log,
)

from constants import (
    DATABASE,
    FULL_PATH_TO_CREDENTIALS,
    HAMSTERWHEEL_LOG_FILE_PATH,
    HOST_LOCAL,
    PORT,
    TABLE_HAMSTERWHEEL,
)


def log(log_path: str, logmsg: str):
    with open(log_path, 'a') as f:
        f.write('\n')
        f.write(logmsg)
        f.close()

# Load credentials to connect to db
with open(FULL_PATH_TO_CREDENTIALS) as f:
    lines = f.readlines()
    f.close()
user = lines[0].split(':')[1].strip()
password = lines[1].split(':')[1].strip()

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=user,
        password=password,
        host=HOST_LOCAL,
        port=PORT,
        database=DATABASE
    )
except mariadb.Error as e:
    msg = f"Error connecting to MariaDB Platform: {e}"
    log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg, printout=True)
    sys.exit(1)

# Get Cursor
cursor = conn.cursor()

# Set Broadcom mode so we can address GPIO pins by number.
io.setmode(io.BCM)

wheelpin = 18
io.setup(wheelpin, io.IN, pull_up_down=io.PUD_UP) 

# While the script runs
cnt = 0
msg = 'Started script...'
log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg, printout=True)
try:
    while True:
            msg = 'Running...'
            log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg, printout=True)
            time.sleep(0.001)
            # When the magnet passes the magnet reed switch, one rotation has happened
            if (io.input(wheelpin) == 0):
                msg = 'MAGNET!'
                log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg, printout=True)
                cursor.execute(f"INSERT INTO {TABLE_HAMSTERWHEEL} (flag) VALUES (True)")
                conn.commit()
                time.sleep(0.01)
except KeyboardInterrupt:
    conn.close()

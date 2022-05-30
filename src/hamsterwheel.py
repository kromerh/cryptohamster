from datetime import datetime
import RPi.GPIO as io
import mariadb
import sys
import time

# Database connection strings
DATABASE = 'cryptohamster'
HOST = 'localhost'
TABLE = 'hamsterwheel'
FULL_PATH_TO_CREDENTIALS = '/home/wilson/credentials.cred'

# Constants for the log file
HOME = '/home/wilson'
HAMSTERWHEEL_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel.log'

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
        host=HOST,
        port=3306,
        database=DATABASE
    )
except mariadb.Error as e:
    msg = f"{datetime.now()} - Error connecting to MariaDB Platform: {e}"
    print(msg)
    log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg)
    sys.exit(1)

# Get Cursor
cursor = conn.cursor()

# Set Broadcom mode so we can address GPIO pins by number.
io.setmode(io.BCM)

wheelpin = 18
io.setup(wheelpin, io.IN, pull_up_down=io.PUD_UP) 

# While the script runs
cnt = 0
msg = f'{datetime.now()} - Started script...'
log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg)
try:
    while True:
            msg = f'{datetime.now()} - Running...'
            print(msg)
            log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg)
            time.sleep(0.001)
            # When the magnet passes the magnet reed switch, one rotation has happened
            if (io.input(wheelpin) == 0):
                msg = f'{datetime.now()} - MAGNET!'
                print(msg)
                log(log_path=HAMSTERWHEEL_LOG_FILE_PATH, logmsg=msg)
                cursor.execute(f"INSERT INTO {TABLE} (flag) VALUES (True)")
                conn.commit()
                time.sleep(0.01)
except KeyboardInterrupt:
    conn.close()

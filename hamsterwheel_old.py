import RPi.GPIO as io
import time
import mariadb
import sys
from datetime import datetime

DATABASE = 'cryptohamster'
HOST = 'localhost'
TABLE = 'hamsterwheel'

# Load credentials to connect to db
with open('./../credentials.cred') as f:
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
    sys.stdout.write(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cursor = conn.cursor()

# Set Broadcom mode so we can address GPIO pins by number.
io.setmode(io.BCM)

wheelpin = 18
io.setup(wheelpin, io.IN, pull_up_down=io.PUD_UP) 

# While the script runs
sys.stdout.write(f'{datetime.now()} - Started script...')
cnt = 0
try:
    while True:
            if (cnt % 1000) == 0:
                sys.stdout.write(f'{datetime.now()} - Running...')
            time.sleep(0.001)
            # When the magnet passes the magnet reed switch, one rotation has happened
            if (io.input(wheelpin) == 0):
                sys.stdout.write('MAGNET!')
                cursor.execute(f"INSERT INTO {TABLE} (flag) VALUES (True)")
                conn.commit()
                time.sleep(0.01)
            cnt += 1
except KeyboardInterrupt:
    conn.close()
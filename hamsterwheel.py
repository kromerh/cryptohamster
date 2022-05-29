from datetime import datetime
import RPi.GPIO as io
import mariadb
import sys
import time

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
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cursor = conn.cursor()

# Set Broadcom mode so we can address GPIO pins by number.
io.setmode(io.BCM)

wheelpin = 18
io.setup(wheelpin, io.IN, pull_up_down=io.PUD_UP) 

# While the script runs
cnt = 0
try:
    while True:
            if (cnt % 2000) == 0:
                print(f'{datetime.now()} - Running...')
            time.sleep(0.001)
            # When the magnet passes the magnet reed switch, one rotation has happened
            if (io.input(wheelpin) == 0):
                print(f'{datetime.now()} - MAGNET!')
                cursor.execute(f"INSERT INTO {TABLE} (flag) VALUES (True)")
                conn.commit()
                time.sleep(0.01)
            cnt += 1
except KeyboardInterrupt:
    conn.close()

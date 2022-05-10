import RPi.GPIO as io
import time
import mariadb
import sys

DATABASE = 'cryptohamster'
HOST = 'localhost'

# Load credentials to connect to db
with open('./../credentials.cred') as f:
    lines = f.readlines()
    f.close()
user = lines[0].split(':')[1]
password = lines[1].split(':')[1]

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
while True:
        print('Running...')
        time.sleep(0.001)
        # When the magnet passes the magnet reed switch, one rotation has happened
        if (io.input(wheelpin) == 0):
            print('MAGNET!')
            cursor.execute(f"INSERT INTO {DATABASE} () VALUES ()")
            time.sleep(0.01)
import RPi.GPIO as io
import time

# Set Broadcom mode so we can address GPIO pins by number.
io.setmode(io.BCM)

wheelpin = 18
io.setup(wheelpin, io.IN, pull_up_down=io.PUD_UP) 

# While the script runs
while True:
        print('Running...')
        time.sleep(0.1)
        # When the magnet passes the magnet reed switch, one rotation has happened
        if (io.input(wheelpin) == 0):
            print('MAGNET!')
            time.sleep(1)
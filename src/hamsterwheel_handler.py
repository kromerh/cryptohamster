import os
import subprocess
import time
from datetime import datetime

REPO = 'wilsonisahamster'
FILENAME_HAMSTERWHEEL = 'hamsterwheel.py'
HOME = '/home/wilson'
HAMSTERWHEEL_PATH = f'{HOME}/{REPO}/{FILENAME_HAMSTERWHEEL}'

HAMSTERWHEEL_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel.log'

ENV_PATH = f'{HOME}/hamsterwheelcaster/bin/python'

HANDLER_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel_handler.log'

# Check if the log file is populating
# Get file size, wait two seconds, get the file size again, and a third time
file_size_1 = os.stat(HAMSTERWHEEL_LOG_FILE_PATH).st_size
# Wait for 2 seconds
time.sleep(2)
# Get new file size
file_size_2 = os.stat(HAMSTERWHEEL_LOG_FILE_PATH).st_size
# Wait for 2 seconds
time.sleep(2)
# Get new file size
file_size_3 = os.stat(HAMSTERWHEEL_LOG_FILE_PATH).st_size

# If all three readings have the same file size, the feed is stale
if (file_size_1 == file_size_2) & (file_size_1 == file_size_3):
    subprocess.call([ENV_PATH, HAMSTERWHEEL_PATH])

# Add to a logfile that the script was started
with open(HANDLER_LOG_FILE_PATH, 'a') as f:
    f.write('/n')
    f.write(f'{datetime.now()} - Started hamsterwheel script. Logfile sizes: [{file_size_1}, {file_size_2}, {file_size_3}]')
    f.close()

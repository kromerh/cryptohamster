import os
import subprocess
import time
from datetime import datetime

# Constants
HOME = '/home/wilson'
FILENAME_RUN_HAMSTERWHEEL = 'run_hamsterwheel.sh'
HAMSTERWHEEL_PATH = f'{HOME}/{FILENAME_RUN_HAMSTERWHEEL}'

HAMSTERWHEEL_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel.log'

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

print(f"Three file size readings are: [{file_size_1}, {file_size_2}, {file_size_3}]")

# If all three readings have the same file size, the feed is stale
if (file_size_1 == file_size_2) & (file_size_1 == file_size_3):
    print(f"Running {HAMSTERWHEEL_PATH}...")
    subprocess.call(['sh', HAMSTERWHEEL_PATH])

# Add to a logfile that the script was started
with open(HANDLER_LOG_FILE_PATH, 'a') as f:
    f.write('\n')
    f.write(f'{datetime.now()} - Started hamsterwheel script. Logfile sizes: [{file_size_1}, {file_size_2}, {file_size_3}]')
    f.close()

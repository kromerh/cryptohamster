import os
import subprocess
import time

from utils import log

from constants import (
    HAMSTERWHEEL_LOG_FILE_PATH,
    HAMSTERWHEEL_PATH,
    HANDLER_LOG_FILE_PATH
)

# Check if the log file is populating for the hamsterwheel script. If it i
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
    # Add to a logfile that the script was started
    msg = f'Starting hamsterwheel script. Logfile sizes: [{file_size_1}, {file_size_2}, {file_size_3}]'
    log(log_path=HANDLER_LOG_FILE_PATH, logmsg=msg)
    subprocess.call(['sh', HAMSTERWHEEL_PATH])

else:
    msg = f'Hamsterwheel script was running. Logfile sizes: [{file_size_1}, {file_size_2}, {file_size_3}]'
    log(log_path=HANDLER_LOG_FILE_PATH, logmsg=msg)

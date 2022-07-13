import os
import subprocess
import time

from utils import log

from constants import (
    CRYPTOHAMSTER_LOG_FILE_PATH,
    CRYPTOHAMSTER_PATH,
    CRYPTOHAMSTER_HANDLER_LOG_FILE_PATH
)

# Check if the log file is populating for the hamsterwheel script. If it i
# Get file size, wait two seconds, get the file size again, and a third time
file_size_1 = os.stat(CRYPTOHAMSTER_LOG_FILE_PATH).st_size
# Wait for 2 seconds
time.sleep(2)
# Get new file size
file_size_2 = os.stat(CRYPTOHAMSTER_LOG_FILE_PATH).st_size
# Wait for 2 seconds
time.sleep(2)
# Get new file size
file_size_3 = os.stat(CRYPTOHAMSTER_LOG_FILE_PATH).st_size

# If all three readings have the same file size, the feed is stale
if (file_size_1 == file_size_2) & (file_size_1 == file_size_3):
    # Add to a logfile that the script was started
    msg = f'Starting cryptohamster script. Logfile sizes: [{file_size_1}, {file_size_2}, {file_size_3}]'
    log(log_path=CRYPTOHAMSTER_HANDLER_LOG_FILE_PATH, logmsg=msg)
    subprocess.call(['sh', CRYPTOHAMSTER_PATH])

else:
    msg = f'Cryptohamster script was running. Logfile sizes: [{file_size_1}, {file_size_2}, {file_size_3}]'
    log(log_path=CRYPTOHAMSTER_HANDLER_LOG_FILE_PATH, logmsg=msg)

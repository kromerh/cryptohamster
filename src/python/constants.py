# RPi paths

# Home directory on the RPi
HOME = '/home/wilson'
REPO = 'wilsonisahamster'
PYTHON = 'src/python'
# Location of the .sh file on the RPi (copied from this repo)
FILENAME_RUN_HAMSTERWHEEL = 'run_hamsterwheel.sh'
HAMSTERWHEEL_PATH = f'{HOME}/{REPO}/{PYTHON}/{FILENAME_RUN_HAMSTERWHEEL}'

# Log files
# Log file for the hamsterwheel sensor code
HAMSTERWHEEL_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel.log'
# Log file for the handler script script
HANDLER_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel_handler.log'

# Database

# Database connection strings
DATABASE = 'cryptohamster'
HOST_LOCAL = 'localhost'  # On the RPi local
HOST_REMOTE = 'wilsonisahamster'  # Sometimes does not work: then use 192.168.1.105
PORT = 3306
FULL_PATH_TO_CREDENTIALS = f'{HOME}/credentials.cred'

# Table for the wheel data
TABLE_HAMSTERWHEEL = 'hamsterwheel'

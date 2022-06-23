# RPi paths

# Home directory on the RPi
HOME = '/home/wilson'
REPO = 'wilsonisahamster'
BASH = 'src/bash'
# Location of the .sh file on the RPi (copied from this repo)
FILENAME_RUN_HAMSTERWHEEL = 'run_hamsterwheel.sh'
HAMSTERWHEEL_PATH = f'{HOME}/{REPO}/{BASH}/{FILENAME_RUN_HAMSTERWHEEL}'

# Log files
PRINTOUT = True
# Log file for the hamsterwheel sensor code
HAMSTERWHEEL_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel.log'
# Log file for the handler script script
HANDLER_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel_handler.log'
# Log file for the decision class
DECISION_LOG_FILE_PATH = f'{HOME}/log/decision.log'

# Database

# Database connection strings
DATABASE = 'cryptohamster'
HOST_LOCAL = 'localhost'  # On the RPi local
HOST_REMOTE = '192.168.1.105'  # Sometimes does not work: then use 192.168.1.105
PORT = 3306
FULL_PATH_TO_CREDENTIALS = f'{HOME}/credentials.cred'

# Table for the wheel data
NO_END_TIME = '0000-00-00 00:00:00.000000'
TABLE_HAMSTERWHEEL = 'hamsterwheel'
DB_TBL = {
    'WALLET': {
        'name': 'wallet',
        'id_col': 'wallet_id'
    },
    'DECISION': {
        'name': 'decision',
        'id_col': 'decision_id'
    },
    'HAMSTERHWEEL': {
        'name': 'hamsterhweel',
        'id_col': 'hamsterhweel_id'
    },
    'DECISION': {
        'name': 'decision',
        'id_col': 'decision_id'
    },
}

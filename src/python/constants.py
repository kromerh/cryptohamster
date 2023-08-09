import numpy as np
from typing import Dict


# SQL Table names
class SQLTable:
    """Class that contains the SQL tables"""

    def __init__(self, table_name: str, columns: Dict[str, str]) -> None:
        self.table_name = table_name
        self.columns = columns


table_raw_hamsterwheel = SQLTable(
    table_name="raw_hamsterwheel", columns={"magnet": "magnet"}
)

table_log = SQLTable(
    table_name="log",
    columns={"event": "event"},
)

table_control = SQLTable(
    table_name="control",
    columns={"variable": "variable", "state": "state"},
)

table_wallet = SQLTable(
    table_name="wallet",
    columns={"currency_symbol": "currency_symbol", "amount": "amount"},
)

table_orderbook = SQLTable(
    table_name="orderbook",
    columns={
        "wallet_id": "wallet_id",
        "price": "price",
        "amount": "amount",
        "type": "type",
    },
)

table_decision_wheel = SQLTable(
    table_name="decision_wheel",
    columns={
        "decision_id": "decision_id",
        "number_hamsterwheel_turns": "number_hamsterwheel_turns",
    },
)

table_decision = SQLTable(
    table_name="decision",
    columns={
        "type": "type",
        "result": "result",
    },
)


# # Control if script is executed on a RPi or remote
# remote = False # Set to False if run on RPi
# # Control if test tables or real tables should be used
# test = False # Set to False during real run

# # RPi paths

# # Home directory on the RPi
# HOME = '/home/wilson'
# REPO = 'cryptohamster'
# BASH = 'src/bash'
# # Location of the .sh file on the RPi (copied from this repo)
# FILENAME_RUN_HAMSTERWHEEL = 'run_hamsterwheel.sh'
# HAMSTERWHEEL_PATH = f'{HOME}/{REPO}/{BASH}/{FILENAME_RUN_HAMSTERWHEEL}'
# FILENAME_RUN_CRYPTOHAMSTER = 'run_cryptohamster.sh'
# CRYPTOHAMSTER_PATH = f'{HOME}/{REPO}/{BASH}/{FILENAME_RUN_CRYPTOHAMSTER}'

# # Log files
# PRINTOUT = True
# # Log file for the hamsterwheel sensor code
# HAMSTERWHEEL_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel.log'
# # Log file for the handler script script
# HAMSTERWHEEL_HANDLER_LOG_FILE_PATH = f'{HOME}/log/hamsterwheel_handler.log'
# CRYPTOHAMSTER_HANDLER_LOG_FILE_PATH = f'{HOME}/log/cryptohamster_handler.log'
# # Log file for the decision class
# if remote:
#     CRYPTOHAMSTER_LOG_FILE_PATH = f'./log/cryptohamster.log'
# else:
#     CRYPTOHAMSTER_LOG_FILE_PATH = f'{HOME}/log/cryptohamster.log'

# # Database

# # Database connection strings
# DATABASE = 'cryptohamster'
# if remote:
#     HOST = '192.168.1.105'
# else:
#     HOST = 'localhost'  # On the RPi local
# PORT = 3306
# FULL_PATH_TO_CREDENTIALS = f'{HOME}/credentials.cred'
# # For execution on another host
# if remote:
#     FULL_PATH_TO_CREDENTIALS = f'./credentials.cred'

# # Table for the wheel data
# NO_END_TIME = '0000-00-00 00:00:00.000000'
# TABLE_HAMSTERWHEEL = 'hamsterwheel'

# if test:
#     DECISION_TBL = 'TEST_decision'
#     WALLET_TBL = 'TEST_wallet'
#     SESSION_TBL = 'TEST_session'
#     HAMSTERWHEEL_TBL = 'TEST_hamsterwheel'
#     TRADEBOOK_TBL = 'TEST_tradebook'
# else:
#     DECISION_TBL = 'decision'
#     WALLET_TBL = 'wallet'
#     SESSION_TBL = 'session'
#     HAMSTERWHEEL_TBL = 'hamsterwheel'
#     TRADEBOOK_TBL = 'tradebook'

# DB_TBL = {
#     'WALLET': {
#         'name': WALLET_TBL,
#         'id_col': 'wallet_id',
#         'currency_symbol_col': 'currency_symbol',
#         'amount_col': 'amount'
#     },
#     'DECISION': {
#         'name': DECISION_TBL,
#         'id_col': 'decision_id',
#         'decision_cycle_col': 'decision_cycle',
#         'session_id_col': 'session_id',
#         'start_time_col': 'start_time',
#         'end_time_col': 'end_time',
#         'hamsterwheel_id_start_col': 'hamsterwheel_id_start',
#         'hamsterwheel_id_end_col': 'hamsterwheel_id_end',
#         'result_col': 'result',
#         'type_col': 'type',
#         'wheel_turns_col': 'wheel_turns',
#     },
#     'HAMSTERWHEEL': {
#         'name': HAMSTERWHEEL_TBL,
#         'id_col': 'hamsterwheel_id',
#         'time_col': 'time',
#     },
#     'SESSION': {
#         'name': SESSION_TBL,
#         'id_col': 'session_id',
#         'start_time_col': 'start_time',
#         'end_time_col': 'end_time',
#         'end_type_col': 'end_type',
#         'hamsterwheel_id_start_col': 'hamsterwheel_id_start',
#     },
#     'TRADEBOOK': {
#         'name': TRADEBOOK_TBL,
#         'id_col': 'tradebook_id',
#         'session_id_col': 'session_id',
#         'decision_id_col': 'decision_id',
#         'buy_sell_col': 'buy_sell',
#         'currency_symbol_col': 'currency_symbol',
#         'cash_amount_col': 'cash_amount',
#         'ccy_amount_col': 'ccy_amount',
#         'ccy_price_col': 'ccy_price',
#         'time_col': 'time'
#     }
# }


# # Decisions
# BUY = 'BUY'
# SELL = 'SELL'
# BUY_SELL = 'buy_sell'
# CURRENCY = 'currency'
# AMOUNT = 'amount'
# TIMEOUT = 'timeout'
# THRESHOLD_DECISION_TIMEOUT = 120

# DECISION_OPTIONS = {
#     BUY_SELL: [],  # is queried from the database
#     CURRENCY: [],  # is queried from the database
#     AMOUNT: [np.round(x, 2) for x in list(np.arange(0.1, 1.05, 0.05))]  # 1 to 100 %
# }

# # Session
# # Session can end either with a BUY or SELL or TIMEOUT
# SESSION_END_TYPES = [BUY, SELL, 'TIMEOUT']
# THRESHOLD_SESSION_TIMEOUT = 3600

# # Currencies
# CASH = 'CASH'

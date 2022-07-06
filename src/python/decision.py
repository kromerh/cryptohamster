from datetime import datetime
from subprocess import IDLE_PRIORITY_CLASS
import numpy as np
import pymysql
import pandas as pd
from typing import Any, Dict, Union, List
import sys

from utils import (
    get_latest_row_by_id,
    log,
)

from constants import (
    CRYPTOHAMSTER_LOG_FILE_PATH,
    DB_TBL,
    BUY,
    SELL,
    CASH,
    PRINTOUT,
    NO_END_TIME,
    DECISION_OPTIONS,
    BUY_SELL,
    CURRENCY,
    AMOUNT,
    TIMEOUT,
    THRESHOLD_DECISION_TIMEOUT
)

from binance import Binance

# MySQL tables
# Wallet: Contains what cryptocurrencices the hamster holds in its wallet

WALLET = 'wallet'

# Threshold in seconds between the latest reading of the wheel and the current time
THRESHOLD_DECISION = 5

# Threshold for the cash amount the hamster holds below which the hamster can only sell
THRESHOLD_CASH = 10 # USD


class Decision():
    """Class that contains logic to come to a decision using the hamsterwheel
    """

    def __init__(
        self,
        ) -> None:
        """Class instantiation.

        """
        # Database tables
        self._db_tbl = DB_TBL
        self._decision_options = DECISION_OPTIONS

    def get_latest_decision(
        self,
        mysql_connection: pymysql.connections.Connection,
        num_rows: int = 1
        ) -> pd.core.series.Series:
        """Method to get the latest entry in the decision table. 

        Args:
            mysql_connection: MySQL connection
            num_rows: How many past decisions should be retrieved, defaults to 1.

        Returns:
            Series with the latest decision.
        """
        table = self._db_tbl['DECISION']['name']
        id_col = self._db_tbl['DECISION']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=mysql_connection,
            table=table,
            id_col=id_col,
            num_rows=num_rows
        )

        return s

    def get_latest_hamsterwheel(
        self,
        mysql_connection: pymysql.connections.Connection,
        ) -> pd.core.series.Series:
        """Method to get the latest entry in the hamsterwheel table. 

        Args:
            mysql_connection: MySQL connection

        Returns:
            Series with the latest hamsterwheel table.
        """
        table = self._db_tbl['HAMSTERWHEEL']['name']
        id_col = self._db_tbl['HAMSTERWHEEL']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=mysql_connection,
            table=table,
            id_col=id_col
        )

        return s
    
    def is_decision_open(self, latest_decision: pd.core.series.Series) -> bool:
        """Method to determine if a decision is open.

        Returns:
            True if there is a running decision, False otherwise.
        """
        end_time_col = self._db_tbl['DECISION']['end_time_col']
        if latest_decision[end_time_col] == NO_END_TIME:
            return True
        else:
            return False

    def is_decision_reached(
        self,
        latest_hamsterwheel: pd.core.series.Series,
        threshold: int = THRESHOLD_DECISION
        ) -> Union[None, int]:
        """Method to check if a decision was reached.

        If the wheel is not turning for more than THRESHOLD_DECISION seconds, a decision is reached. 

        Args:
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
            threshold: Time in seconds after which the decision is considered reached.

        Returns:
            True if decision is reached, False otherwise.
        """
        time_col = self._db_tbl['HAMSTERWHEEL']['time_col']
        id_col = self._db_tbl['HAMSTERWHEEL']['id_col']
        # Time difference between last reading and now
        time_diff = (datetime.now() - latest_hamsterwheel[time_col]).total_seconds()

        if time_diff > threshold:
            # Decision is reached
            # Get the id
            latest_wheel_id = latest_hamsterwheel.name
            # Add to the log
            logmsg = f'Decision reached, time difference was {time_diff} seconds. Hamsterwheel id is {latest_wheel_id}'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            # Update the closing decision 
            return True
        
        return False

    def calculate_num_of_wheel_turns(
        self,
        mysql_connection: pymysql.connections.Connection,
        latest_decision: pd.core.series.Series,
        latest_hamsterwheel: pd.core.series.Series,
        threshold_deadtime: float = 0.7
        ) -> int:
        """Method to calculate the number of wheel turns in the recent decision cycle.

        Args:
            mysql_connection: MySQL connection
            latest_decision: Latest decision row.
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
            threshold_deadtime: Sensor readout is faster than passing of the magnet. Entries in the database
                that are below this threshold apart from another will be removed.

        Returns:
            Number of wheel turns.
        """
        table = self._db_tbl['HAMSTERWHEEL']['name']
        id_col = self._db_tbl['HAMSTERWHEEL']['id_col']
        decision_hamsterwheel_id_start = self._db_tbl['DECISION']['hamsterwheel_id_start_col']
        hamsterhweel_time_col = self._db_tbl['HAMSTERWHEEL']['time_col']

        # Get the latest hamsterwheel ids
        begin_wheel_id = latest_decision[decision_hamsterwheel_id_start]
        end_wheel_id = latest_hamsterwheel.name

        # Retrieve the hamsterwheel data for these ids        
        qry = f'SELECT * FROM {table} ' +\
            f'WHERE {id_col} BETWEEN {begin_wheel_id} and {end_wheel_id}'
        
        wheel_data = pd.read_sql(
            sql=qry,
            con=mysql_connection,
            index_col=id_col
        )
        # Remove entries from the dataset that are very short apart
        # The hamster cannot run that fast, see EDA notebook. These readings come from the sensor readout being
        # faster than the magnet can pass the sensor.
        wheel_data['diff'] = np.abs((wheel_data[hamsterhweel_time_col] - wheel_data[hamsterhweel_time_col].shift(1)).dt.total_seconds())
        # Get a True/False series using the threshold
        mask_deadtime = wheel_data['diff'] > threshold_deadtime
        num_wheelturns = mask_deadtime.sum()

        return num_wheelturns

    def determine_decision(
        self,
        wallet: Dict[str, float],
        latest_decision: pd.core.series.Series,
        num_wheelturns: int
        ) -> str:
        """Method to make a decision given the number of wheel_counts and the current decision mode.

        Args:
            latest_decision: Latest decision row.
            num_wheelturns: Number of wheelturns in the decision cycle.

        Returns:
            Decision outcome.
        """
        type_col = self._db_tbl['DECISION']['type_col']
        # Get the current decision
        current_decision = latest_decision[type_col]

        # Update the decision options
        self.update_decision_options(wallet=wallet, get_currencies=True)

        # Lookup the list of possible decision from the dictionary
        decision_list = self._decision_options[current_decision]

        # Make the decision using the number of wheelturns
        # Remove 1 turn to account for the 0th wheel turn
        num_wheelturns = num_wheelturns - 1
        # Repeat the decision list enough times to fit the wheelturn in
        # Add 1 for safety
        multiplier = int(np.ceil(num_wheelturns/len(decision_list))) + 1
        multiplied_list = decision_list * multiplier

        result = multiplied_list[num_wheelturns]
        
        logmsg = f'Decision determined: {result}'
        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return result

    def update_decision_closed(
        self,
        mysql_connection: pymysql.connections.Connection,
        latest_decision: pd.core.series.Series,
        latest_hamsterwheel: pd.core.series.Series,
        wheel_turns: Union[None, int],
        result: Union[None, str]
        ) -> None:
        """Method to update the decision after decision was reached

        Args:
            mysql_connection: MySQL connection
            latest_decision: Latest decision row.
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
            wheel_turns: Number of wheel turns between start and end time.
            result: Result of the decision.
        
        Returns:
            None.
        """
        # Get the latest hamsterwheel id
        latest_wheel_id = latest_hamsterwheel.name

        table = self._db_tbl['DECISION']['name']
        end_time_col = self._db_tbl['DECISION']['end_time_col']
        id_col = self._db_tbl['DECISION']['id_col']
        hamsterwheel_id_end_col = self._db_tbl['DECISION']['hamsterwheel_id_end_col']
        result_col = self._db_tbl['DECISION']['result_col']
        wheel_turns_col = self._db_tbl['DECISION']['wheel_turns_col']


        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'UPDATE {table} ' +\
              f'SET ' +\
              f'{end_time_col} = \"{now}\", ' +\
              f'{hamsterwheel_id_end_col} = {latest_wheel_id}, ' +\
              f'{result_col} = \"{result}\", ' +\
              f'{wheel_turns_col} = {wheel_turns} ' +\
              f'WHERE {id_col} = {latest_decision.name}'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()   
            logmsg = f'Updated decision as closed with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update decision as closed with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )

    def get_next_decision(
        self,
        latest_decision: pd.core.series.Series,
        ) -> str:
        """Method to start a new decision because the previous one is closed.

        Args:
            latest_decision: Latest decision row.
        
        Returns:
            Next decision.
        """
        # Check what the last closed decision was
        type_col = self._db_tbl['DECISION']['type_col']
        latest_decision_type = latest_decision[type_col]
        # Check the latest result, if it was a timeout, we go back to BUY_SELL
        result_col = self._db_tbl['DECISION']['result_col']
        latest_decision_result = latest_decision[result_col]

        # Move to the next decision option
        if latest_decision_type == BUY_SELL:
            next_decision_type = CURRENCY
        elif latest_decision_type == CURRENCY:
            next_decision_type = AMOUNT
        elif latest_decision_type == AMOUNT:
            next_decision_type = BUY_SELL
        else:
            logmsg = f'Decision type {latest_decision_type} not understood.'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            raise ValueError(logmsg)
        
        if latest_decision_result == TIMEOUT:
            next_decision_type = BUY_SELL
        
        return next_decision_type

    def start_new_decision(
        self,
        mysql_connection: pymysql.connections.Connection,
        next_decision_type: str,
        session_id: int,
        latest_hamsterwheel_id: int,
        latest_decision: pd.core.series.Series,
        ) -> pd.core.series.Series:
        """Method to start a new decision. Returns the next decision series.

        Args:
            mysql_connection: MySQL connection
            next_decision: Next decision type.
            session_id: Current session id.
            latest_hamsterwheel_id: Latest hamsterwheel id from the database.
            latest_decision: Latest decision from the database.
        
        Returns:
            None.
        """
        table = self._db_tbl['DECISION']['name']
        start_time_col = self._db_tbl['DECISION']['start_time_col']
        session_id_col = self._db_tbl['DECISION']['session_id_col']
        hamsterwheel_id_start_col = self._db_tbl['DECISION']['hamsterwheel_id_start_col']
        type_col = self._db_tbl['DECISION']['type_col']
        decision_cycle_col = self._db_tbl['DECISION']['decision_cycle_col']

        # Get the current decision cycle
        if latest_decision is None:
            decision_cycle = 1
        else:      
            if next_decision_type == BUY_SELL:  
                # Check if the hamster holds cash to buy
                # If buy or sell, increase the decision cycle by one
                decision_cycle = int(latest_decision[decision_cycle_col]) + 1
            else:
                decision_cycle = int(latest_decision[decision_cycle_col])

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{session_id_col}, ' +\
              f'{decision_cycle_col}, ' +\
              f'{type_col}, ' +\
              f'{start_time_col}, ' +\
              f'{hamsterwheel_id_start_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'{session_id}, ' +\
              f'{decision_cycle}, ' +\
              f'\"{next_decision_type}\", ' +\
              f'\"{now}\", ' +\
              f'{latest_hamsterwheel_id} ' +\
              f')'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()
            logmsg = f'Updated decision as new decision with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update decision as new decision with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )

    def is_decision_timeout(
        self,
        latest_decision: pd.core.series.Series,
        latest_hamsterwheel: pd.core.series.Series,
        threshold: int = THRESHOLD_DECISION_TIMEOUT
        ) -> bool:
        """Method to check if an open decision is timed out.

        If the last turn of the wheel is more than 120 seconds ago, the decision is considered timed out and will be closed.

        Args:
            latest_decision: Latest decision row.
            latest_decision: Latest hamsterwheel row.
            threshold: Time in seconds after which the decision is considered timed out.

        Returns:
            True if decision is timeout, False otherwise.
        """
        time_col = self._db_tbl['HAMSTERWHEEL']['time_col']
        # Time difference between last reading of the hamsterwheel and now
        time_diff = (datetime.now() - latest_hamsterwheel[time_col]).total_seconds()

        if time_diff > threshold:
            # Decision is timed out
            # Get the id
            latest_decision_id = latest_decision.name
            # Add to the log
            logmsg = f'Decision timed out, time difference was {time_diff} seconds. Decision id is {latest_decision_id}.'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            return True
        
        return False
    
    def update_decision_options(
        self,
        wallet: Dict[str, float],
        get_currencies: bool = False
    ) -> None:
        """Method to get the decision options.

        Args:
            wallet: Wallet of the hamster.
            get_currencies: Flag to control to also retrieve currencies from Binance to buy.
        
        Returns:
            Dictionary with the decision options.
        """
        # Currencies the hamster holds
        currencies = list(wallet.keys())
        currencies.remove(CASH)

        # Cash amount the hamster holds
        cash = wallet[CASH]

        # BUY_SELL
        # If the hamster has no currencies, we can only buy
        if len(currencies) == 0:
            if cash > 0:
                self._decision_options[BUY_SELL] = [BUY]
            # Hamster is broke
            else:
                logmsg = f'Hamster is broke! No currencies and no cash.'
                log(
                    log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                    logmsg=logmsg,
                    printout=PRINTOUT
                )
                sys.exit()
        else:
            # If the hamster has no cash, we can only sell
            if cash < THRESHOLD_CASH:
                self._decision_options[BUY_SELL] = [SELL]
                logmsg = f'Hamster is out of cash, can only sell.'
                log(
                    log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                    logmsg=logmsg,
                    printout=PRINTOUT
                )
            else:
                self._decision_options[BUY_SELL] = [BUY, SELL]
        
        # CURRENCY
        if get_currencies:
            # Retrieve the list of available currencies from binance
            currencies = Binance().get_available_currencies()
            self._decision_options[CURRENCY] = currencies

    def get_decision_options(self) -> Dict[str, List[Any]]:
        """Method to return the decision options.

        Returns:
            Dictionary of the decision options.
        """
        return self._decision_options

    def check_all_decisions_for_trade(
        self,
        past_decisions: pd.DataFrame,
        mysql_connection: pymysql.connections.Connection,
    ) -> bool:
        """Method to check if all decisions for a trade have been concluded.

        These are:
            * BUY_SELL
            * CURRENCY
            * AMOUNT

        Args:
            past_decisions: Dataframe with the last three decisions.
            mysql_connection: MySQL connection
        
        Returns:
            True if all the decisions necessary for a trade have been reached, False otherwise.
        """
        # List of decisions
        decisions_list = list(self._decision_options.keys())
        
        # Check if all the decisions are there
        type_col = self._db_tbl['DECISION']['type_col']
        decision_cycle_col = self._db_tbl['DECISION']['decision_cycle_col']
        # Check if there is only on decision cycle in the past three rows
        if len(set(list(past_decisions[decision_cycle_col]))) == 1:
            if set(decisions_list) == set(list(past_decisions[type_col])):
                response = True
                logmsg = f'Decisions complete to start a trade.'
            else:
                logmsg = f'Decisions not complete to start a trade.'
                response= False
        else:
            logmsg = f'Decisions not complete to start a trade.'
            response= False
        
        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return response

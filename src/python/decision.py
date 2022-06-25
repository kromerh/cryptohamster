from datetime import datetime
from subprocess import IDLE_PRIORITY_CLASS
import numpy as np
import pymysql
import pandas as pd
from typing import Dict, Union

from utils import (
    get_latest_row_by_id,
    log,
)

from constants import (
    DECISION_LOG_FILE_PATH,
    DB_TBL,
    PRINTOUT,
    NO_END_TIME,
    DECISION_OPTIONS,
    BUY_SELL,
    CURRENCY,
    AMOUNT,
)

# MySQL tables
# Wallet: Contains what cryptocurrencices the hamster holds in its wallet

WALLET = 'wallet'

# Threshold in seconds between the latest reading of the wheel and the current time
THRESHOLD_DECISION = 5

# Threshold for the cash amount the hamster holds below which the hamster is broke
THRESHOLD_CASH = 10 # USD


class Decision():
    """Class that contains logic to come to a decision using the hamsterwheel
    """
    
    def __init__(
        self,
        mysql_kwargs: Dict[str, str],

        ) -> None:
        """Class instantiation.

        Args:
            mysql_kwargs: MySQL keyword arguments

        """
        # List of available decisions
        self._decision_options = DECISION_OPTIONS
        # Which decision is currently being processed
        self._current_decision_number = 0
        # Instantiate decision lists
        self._currency = []
        self._amount = AMOUNT
        # MySQL tables
        self._db_tbl = DB_TBL
        self._mysql_kwargs = Decision._validate_mysql_kwargs(mysql_kwargs)

        # Connect to the db
        self.connect_to_database()

        # Get wallet
        self._wallet = self.get_wallet()
    

    def get_latest_decision(self) -> pd.core.series.Series:
        """Method to get the latest entry in the decision table. 

        Returns:
            Series with the latest decision.
        """
        table = self._db_tbl['DECISION']['table']
        id_col = self._db_tbl['DECISION']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=self._mysql_connection,
            table=table,
            id_col=id_col
        )

        return s


    def get_latest_hamsterwheel(self) -> pd.core.series.Series:
        """Method to get the latest entry in the hamsterwheel table. 

        Returns:
            Series with the latest hamsterwheel table.
        """
        table = self._db_tbl['HAMSTERWHEEL']['table']
        id_col = self._db_tbl['HAMSTERWHEEL']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=self._mysql_connection,
            table=table,
            id_col=id_col
        )

        return s
    

    def is_decision_open(self, latest_decision: pd.core.series.Series) -> bool:
        """Method to determine if a decision is open.

        Returns:
            True if there is a running decision, False otherwise.
        """
        if latest_decision['end_time'] == NO_END_TIME:
            return False
        else:
            return True


    def is_decision_reached(
        self,
        latest_hamsterwheel: pd.core.series.Series,
        threshold: int
        ) -> Union[None, int]:
        """Method to check if a decision was reached.

        If the wheel is not turning for more than THRESHOLD_DECISION seconds, a decision is reached. 

        Args:
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
            threshold: Time in seconds after which the decision is considered reached.

        Returns:
            True if decision is reached, False otherwise.
        """
        # Time difference between last reading and now
        time_diff = (datetime.now() - latest_hamsterwheel['time']).total_seconds()

        if time_diff > threshold:
            # Decision is reached
            # Get the id
            latest_wheel_id = latest_hamsterwheel['hamsterwheel_id']
            # Add to the log
            logmsg = f'Decision reached, time difference was {time_diff} seconds. Hamsterwheel id is {latest_wheel_id}'
            log(
                log_path=DECISION_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            # Update the closing decision 
            return True
        
        return False


    def calculate_num_of_wheel_turns(
        self,
        latest_decision: pd.core.series.Series,
        latest_hamsterwheel: pd.core.series.Series,
        threshold_deadtime: float = 0.7
        ) -> int:
        """Method to calculate the number of wheel turns in the recent decision cycle.

        Args:
            latest_decision: Latest decision row.
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
            threshold_deadtime: Sensor readout is faster than passing of the magnet. Entries in the database
                that are below this threshold apart from another will be removed.

        Returns:
            Number of wheel turns.
        """
        # Get the latest hamsterwheel ids
        begin_wheel_id = latest_decision['hamsterwheel_id_start']
        end_wheel_id = latest_hamsterwheel['hamsterwheel_id']

        # Retrieve the hamsterwheel data for these ids
        table = self._db_tbl['HAMSTERWHEEL']['name']
        id_col = self._db_tbl['HAMSTERWHEEL']['id_col']
        qry = f'SELECT * FROM {table} ' +\
            f'WHERE {id_col} BETWEEN {begin_wheel_id} and {end_wheel_id}'
        
        wheel_data = pd.read_sql(
            sql=qry,
            con=self._mysql_connection,
            index_col=id_col
        )
        # Remove entries from the dataset that are very short apart
        # The hamster cannot run that fast, see EDA notebook. These readings come from the sensor readout being
        # faster than the magnet can pass the sensor.
        wheel_data['diff'] = (wheel_data['time'].shift(1) - wheel_data['time']).dt.total_seconds()
        # Get a True/False series using the threshold
        mask_deadtime = wheel_data['diff'] > threshold_deadtime
        num_wheelturns = mask_deadtime.sum()

        return num_wheelturns


    def determine_decision(
        self,
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
        # Get the current decision
        current_decision = latest_decision['type']

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
        
        logmsg = f'Decision determined: ' + result
        log(
            log_path=DECISION_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return result


    def update_decision_closed(
        self,
        latest_decision: pd.core.series.Series,
        latest_hamsterwheel: pd.core.series.Series,
        wheel_turns: int,
        result: str
        ) -> None:
        """Method to update the decision after decision was reached

        Args:
            latest_decision: Latest decision row.
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
            wheel_turns: Number of wheel turns between start and end time.
            result: Result of the decision.
        
        Returns:
            None.
        """
        # Get the latest hamsterwheel id
        latest_wheel_id = latest_hamsterwheel['hamsterwheel_id']

        table = self._db_tbl['DECISION']['name']
        end_time_col = self._db_tbl['DECISION']['end_time_col']
        id_col = self._db_tbl['DECISION']['id_col']
        hamsterwheel_id_end_col = self._db_tbl['DECISION']['hamsterwheel_id_end_col']
        result_col = self._db_tbl['DECISION']['result_col']
        wheel_turns_col = self._db_tbl['DECISION']['wheel_turns_col']


        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'UPDATE {table} ' +\
              f'SET ' +\
              f'{end_time_col} = \"{now}\" ' +\
              f'{hamsterwheel_id_end_col} = {latest_wheel_id} ' +\
              f'{result_col} = {result} ' +\
              f'{wheel_turns_col} = {wheel_turns} ' +\
              f'WHERE {id_col} = {latest_decision.name}'
        try:
            cursor = self._mysql_connection.cursor()
            cursor.execute(qry)
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update decision as closed with query: ' + qry
            log(
                log_path=DECISION_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        finally:
            self._mysql_connection.commit()   
            logmsg = f'Updated decision as closed with query: ' + qry
            log(
                log_path=DECISION_LOG_FILE_PATH,
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
        latest_decision_type = latest_decision['type']

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
                log_path=DECISION_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            raise ValueError(logmsg)
        
        return next_decision_type


    def update_decision_started(
        self,
        next_decision_type: str,
        session_id: int,
        latest_hamsterwheel: pd.core.series.Series,
        ) -> pd.core.series.Series:
        """Method to update the decision table with the next decision. Returns the next decision series.

        Args:
            next_decision: Next decision type.
            session_id: Current session id.
            latest_hamsterwheel: Series with the latest hamsterwheel from the database.
        
        Returns:
            None.
        """
        # Get the latest hamsterwheel id
        latest_wheel_id = latest_hamsterwheel['hamsterwheel_id']

        table = self._db_tbl['DECISION']['name']
        start_time_col = self._db_tbl['DECISION']['start_time_col']
        session_id_col = self._db_tbl['DECISION']['session_id_col']
        hamsterwheel_id_start_col = self._db_tbl['DECISION']['hamsterwheel_id_start_col']
        type_col = self._db_tbl['DECISION']['type_col']


        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{session_id_col}, ' +\
              f'{type_col}, ' +\
              f'{start_time_col}, ' +\
              f'{hamsterwheel_id_start_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'{session_id}, ' +\
              f'{next_decision_type}, ' +\
              f'{now}, ' +\
              f'{latest_wheel_id} ' +\
              f')'
        try:
            cursor = self._mysql_connection.cursor()
            cursor.execute(qry)
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update decision as new decision with query: ' + qry
            log(
                log_path=DECISION_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        finally:
            self._mysql_connection.commit()   
            logmsg = f'Updated decision as new decision with query: ' + qry
            log(
                log_path=DECISION_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
    

    


    @classmethod
    def _validate_mysql_kwargs(self, mysql_kwargs: Dict[str, str]) -> Union[Dict[str, str], None]:
        """Method to validate the MySQL keyword arguments.

        Returns:
            mysql_kwargs if the dictionary passes the validation.

        Raises:
            ValueError if the expected keywords are not present in the mysql_kwargs
        """
        expected_keys = ['host', 'user', 'password', 'db', 'port']
        expected_keys.sort()
        check_list = list(mysql_kwargs.keys())
        check_list.sort()

        try:
            assert expected_keys == check_list
        except AssertionError:
            raise ValueError(f"User input invalid. Not all mysql keywords in the keyword list.")
        
        return mysql_kwargs

    
    def connect_to_database(self) -> None:
        """Method to get the MySQL connection to the database.
        """
        self._mysql_connection = pymysql.connect(
            charset='utf8',
            **self._mysql_kwargs
        )


    def get_wallet(self) -> pd.DataFrame:
        """Method to receive the currencies and amount that the hamster holds.

        Returns:    
            Dataframe with owned currencies and values.
        """
        wallet_table = self._db_tbl['WALLET']
        qry = f'SELECT * FROM {wallet_table}'
        df = pd.read_sql(
            sql=qry,
            con=self._mysql_connection,
            index_col='wallet_id'
        )

        return df


    


    def get_decision_number(self):
        """Method to get the decision number.

        After a decision is reached, the number of turns of the wheel in the recent decision period is taken. With this
        number, the current decision list is evaluated to reach a decision.
        """


    def go_to_next_decision(self):
        """Method to go to the next decision in the decision list.
        """
        # Go to the next decision key, if larger than the number of keys, go to key 1
        next_key = self._current_decision_number + 1
        if next_key >= len(self._current_decision_number.keys()):
            next_key = 1

        next_decision = self._decisions[str(next_key)]

        # Add to the log
        logmsg = f'Current key is {self._current_decision_number}, next key is {next_key}, next decision is ' +\
            f'{self._decisions[str(next_key)]}'
        log(
            log_path=DECISION_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return next_decision
    

    def set_next_decision(self, next_decision: str):
        """Method to set the next decision.
        """
        # Cash
        mask = self._wallet['CCY'] == 'CASH'
        cash = self._wallet.loc[mask, 'Balance'].values[0]
        # Number of currencies
        CCYs = self._wallet.loc[~mask].count()
        num_currencies = CCYs # all in the wallet except the cash

        # If the next decision is buy or sell
        if next_decision == self._decisions['1']:
            if (cash < THRESHOLD_CASH) & (num_currencies > 0):
                # If the hamster has no cash, the decision is sell
                logmsg = f'Hamster has no cash, cash is {cash}, decision is sell.'
                log(
                    log_path=DECISION_LOG_FILE_PATH,
                    logmsg=logmsg,
                    printout=PRINTOUT
                )
                self._buy_sell = 'SELL'
                # Return the next decision
                next_decision = self.go_to_next_decision()
            elif num_currencies == 0:
                # If the hamster has no currencies, but some cash the decision is buy
                logmsg = f'Hamster has no currencies but cash, number of currencies are {num_currencies}, decision is buy.'
                log(
                    log_path=DECISION_LOG_FILE_PATH,
                    logmsg=logmsg,
                    printout=PRINTOUT
                )
                self._buy_sell = 'BUY'
                # Return the next decision
                next_decision = self.go_to_next_decision()
            if (cash < THRESHOLD_CASH) & (num_currencies == 0):
                logmsg = f'Hamster is broke. Cash is {cash}, number of currencies are {num_currencies}.' +\
                    ' Hamster is done investing!'
                log(
                    log_path=DECISION_LOG_FILE_PATH,
                    logmsg=logmsg,
                    printout=PRINTOUT
                )
                # Hamster is broke
                # Add to the decision table that the hamster is broke
                pass
        
        return next_decision


    def make_decision(self, decision_number: int) -> None:
        """Method to make a decision after the hamster stopped in the wheel.

        Args:
            decision_number: Number that is used to define which item in the list to decide on.

        """
        # Get what the current decision is


    def main(self):
        """Main method to run the decision wheel using the hamsterwheel!
        """
        # Read the current decision status from the database

    
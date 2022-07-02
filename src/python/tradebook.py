import pymysql
from datetime import datetime
from typing import Dict

from utils import (
    log
)

from constants import (
    DB_TBL,
    CRYPTOHAMSTER_LOG_FILE_PATH,
    PRINTOUT,
    DECISION_OPTIONS,
    BUY_SELL
)

class Tradebook:
    """Class to manage the tradebook.

    The tradebook keeps track of the cryptocurrencies bought/sold by the hamster.
    """
    
    def __init__(
        self,
        ) -> None:
        """Class instantiation.
        """
        # Database tables
        self._db_tbl = DB_TBL
        # Decision options
        self._buy_sell_decision = DECISION_OPTIONS[BUY_SELL]
        

    def calculate_amount(
        self,
        wallet: Dict[str , float],
        available_funds: float,
        amount_percentage: float,
        buy_sell_result: str,
        currency: str,
        ) -> float:
        """Method to calculate the amount of a currency to buy or sell.

        Args:
            wallet: Wallet of the hamster.
            available_funds: Number that is USD either available cash (in buy scenario) or available
                amount in a currency.
            amount_percentage: Percentage value that comes from the decision wheel.
            buy_sell_result: Result of the buy / sell decision.
            currency: Cryptocurrency to buy.
        
        Returns:
            Amount to either buy or sell.
        """
        # If buy
        if buy_sell_result == self._buy_sell_decision['BUY']:
            # Check how much cash the hamster holds
            cash = wallet['CASH']
            # Amount to buy is cash times the amount percentage from the decision
            amount = amount_percentage * cash
        
        elif buy_sell_result == self._buy_sell_decision['SELL']:
            # if sell
            # Get how much of the currency the hamster holds
            available_funds = wallet[currency]
            # Calculate amount to sell
            amount = amount_percentage * available_funds

        else:
            # If neither, something went wrong
            logmsg = f'Error: Buy sell result {buy_sell_result} not understood.'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            amount = -1


        return amount


    def update_tradebook(
        self,
        mysql_connection: pymysql.connections.Connection,
        session_id: int,
        decision_id: int,
        buy_sell_result: str,
        currency: str,
        amount: float
        ) -> None:
        """Method to update the tradebook. Updates the MySQL table.

        Args:
            mysql_connection: MySQL connection
            session_id: Active session id.
            decision_id: Active decision id.
            buy_sell_result: Result of the buy / sell decision.
            currency: Cryptocurrency to buy.
            amount: Amount of the currency to buy.
        
        Returns:
            None.
        """
        table = self._db_tbl['TRADEBOOK']['name']
        session_id_col = self._db_tbl['TRADEBOOK']['session_id_col']
        decision_id_col = self._db_tbl['TRADEBOOK']['decision_id_col']
        buy_sell_col = self._db_tbl['TRADEBOOK']['buy_sell_col']
        currency_symbol_col = self._db_tbl['TRADEBOOK']['currency_symbol_col']
        amount_col = self._db_tbl['TRADEBOOK']['amount_col']
        time_col = self._db_tbl['TRADEBOOK']['time_col']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{session_id_col}, ' +\
              f'{decision_id_col}, ' +\
              f'{buy_sell_col}, ' +\
              f'{currency_symbol_col}, ' +\
              f'{amount_col}, ' +\
              f'{time_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'{session_id}, ' +\
              f'{decision_id}, ' +\
              f'\"{buy_sell_result}\", ' +\
              f'\"{currency}\", ' +\
              f'{amount}, ' +\
              f'\"{now}\" ' +\
              f')'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()   
            logmsg = f'Updated tradebook with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update tradebook with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )



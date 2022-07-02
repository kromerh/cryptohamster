import pymysql
from datetime import datetime
from typing import Dict
import pandas as pd

from utils import (
    log
)

from constants import (
    DB_TBL,
    CRYPTOHAMSTER_LOG_FILE_PATH,
    PRINTOUT
)

class Wallet:
    """Class to manage the wallet.

    The wallet keeps track of the positions the hamster holds.
    """
    
    def __init__(
        self,
        ) -> None:
        """Class instantiation.
        """
        # Database tables
        self._db_tbl = DB_TBL
        

    def get_wallet(
        self,
        mysql_connection: pymysql.connections.Connection,
    ) -> Dict[str, float]:
        """Method to retrieve the wallet from the database.

        Args:
            mysql_connection: MySQL connection.

        Returns:
            Dictionary with {symbol: amount}.
        """
        table = self._db_tbl['WALLET']['name']
        id_col = self._db_tbl['WALLET']['id_col']
        currency_symbol_col = self._db_tbl['WALLET']['currency_symbol_col']
        amount_col = self._db_tbl['WALLET']['amount_col']

        qry = f'SELECT * FROM {table}'
        
        wallet = pd.read_sql(
            sql=qry,
            con=mysql_connection,
            index_col=id_col
        )

        return wallet

    def add_new_currency(
        self,
        mysql_connection: pymysql.connections.Connection,
        currency: str,
        amount: float
    ) -> None:
        """Method to add a new currency to the wallet.

        Args:
            mysql_connection: MySQL connection.
            buy_sell_result: Result of the buy / sell decision.
            currency: Cryptocurrency to buy.
            amount: Amount of the currency to buy.

        Returns:
            None.
        """
        table = self._db_tbl['WALLET']['name']
        currency_symbol_col = self._db_tbl['WALLET']['currency_symbol_col']
        amount_col = self._db_tbl['WALLET']['amount_col']

        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{currency_symbol_col}, ' +\
              f'{amount_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'\"{currency}\", ' +\
              f'{amount} ' +\
              f')'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()   
            logmsg = f'Added new currency with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed to add new currency with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )

    def update_existing_currency(
        self,
        mysql_connection: pymysql.connections.Connection,
        wallet: Dict[str, float],
        currency: str,
        buy_sell_result: str,
        amount: float
    ) -> Dict[str, float]:
        """Method to update an existing currency in the wallet.

        Can either be selling a currency or adding more to it.

        Args:
            mysql_connection: MySQL connection.
            wallet: Wallet of the hamster.
            currency: Currency to modify.
            buy_sell_result: Result of the buy / sell decision.
            amount: Amount to buy or sell.

        Returns:
            None.
        """    
        # Get how much the hamster has of the currency
        current_amount  = wallet[currency]

        # Add or subtract
        if buy_sell_result == self._buy_sell_decision['BUY']:
            # Add to the current amount
            new_amount = current_amount + amount
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
        if buy_sell_result == self._buy_sell_decision['BUY']:

        table = self._db_tbl['WALLET']['name']
        currency_symbol_col = self._db_tbl['WALLET']['currency_symbol_col']
        amount_col = self._db_tbl['WALLET']['amount_col']

        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{currency_symbol_col}, ' +\
              f'{amount_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'\"{currency}\", ' +\
              f'{amount} ' +\
              f')'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()   
            logmsg = f'Added new currency with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed to add new currency with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )

    def update_wallet(
        self,
        mysql_connection: pymysql.connections.Connection,
        buy_sell_result: str,
        currency: str,
        amount: float
    ) -> Dict[str, float]:
        """Method to update the wallet. Updates the database table and the wallet object.

        Returns:
            Dictionary with {symbol: amount}.
        """
        # If currency is in the wallet, we need to update
        if currency in list(self._wallet.keys()):
            self.update_existing_currency
        # Else the currency is new, we need to add new currency
        else:

        table = self._db_tbl['WALLET']['name']
        id_col = self._db_tbl['WALLET']['id_col']
        currency_symbol_col = self._db_tbl['WALLET']['currency_symbol_col']
        amount_col = self._db_tbl['WALLET']['amount_col']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'UPDATE {table} ' +\
              f'SET ' +\
              f'{currency_symbol_col} = \"{now}\", ' +\
              f'{amount_col} = {latest_wheel_id}, ' +\
              f'WHERE {currency_symbol_col} = {latest_decision.name}'
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
            logmsg = f'Failed update decision as closed with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )



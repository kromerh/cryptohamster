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
    PRINTOUT,
    CASH
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
        wallet: Dict[str, float],
        latest_trade: pd.core.series.Series
    ) -> None:
        """Method to add a new currency to the wallet.

        Can either be selling a currency or adding more to it.

        Args:
            mysql_connection: MySQL connection.
            wallet: Wallet of the hamster.
            latest_trade: Latest trade series.

        Returns:
            None.
        """
        # What currency we will add
        trdbk_currency_symbol_col = self._db_tbl['TRADEBOOK']['currency_symbol_col']
        currency = latest_trade[trdbk_currency_symbol_col]
        # How much currency do we want to add
        trdbk_ccy_amount_col = self._db_tbl['TRADEBOOK']['ccy_amount_col']
        ccy_amount = latest_trade[trdbk_ccy_amount_col]
        # How much cash we are spending
        trdbk_cash_amount_col = self._db_tbl['TRADEBOOK']['cash_amount_col']
        cash_amount = latest_trade[trdbk_cash_amount_col]
        # Currenct cash
        current_cash_amount = wallet[CASH]
        new_cash_amount = current_cash_amount - cash_amount

        # Get query to insert new currency
        qry_ccy = self.get_qry_new_ccy(ccy_amount=ccy_amount, currency=currency)
        # Get query to change cash
        qry_cash = self.get_qry_update_wallet_cash(new_cash_amount=new_cash_amount)

        try:
            cursor = mysql_connection.cursor()
            if qry_ccy:
                cursor.execute(qry_ccy)
            if qry_cash:
                cursor.execute(qry_cash)
            mysql_connection.commit()   
            logmsg = f'Updated wallet with new currency query: ' + qry_ccy + qry_cash
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update wallet with new currency query: ' + qry_ccy + qry_cash
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )


    def get_qry_new_ccy(
        self,
        ccy_amount: float,
        currency: str
    ) -> str:
        """Method to get the query to add a new currency to the wallet.

        Args:
            new_ccy_amount: Amount in the currency.
            currency: Currency to add

        Returns
            Query to update wallet.
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
              f'{ccy_amount} ' +\
              f')'

        return qry

    def update_existing_currency(
        self,
        mysql_connection: pymysql.connections.Connection,
        wallet: Dict[str, float],
        latest_trade: pd.core.series.Series
    ) -> None:
        """Method to update an existing currency in the wallet.

        Can either be selling a currency or adding more to it.

        Args:
            mysql_connection: MySQL connection.
            wallet: Wallet of the hamster.
            latest_trade: Latest trade series.

        Returns:
            None.
        """    
        # Get the currency
        currency_symbol_col = self._db_tbl['TRADEBOOK']['currency_symbol_col']
        currency = latest_trade[currency_symbol_col]
        # Get the buy/sell decision
        buy_sell_col = self._db_tbl['TRADEBOOK']['buy_sell_col']
        buy_sell_result = latest_trade[buy_sell_col]

        # Get the CCY
        ccy_amount_col = self._db_tbl['TRADEBOOK']['ccy_amount_col']
        # CCY amount change
        ccy_amount = latest_trade[ccy_amount_col]
        # Get the cash
        cash_amount_col = self._db_tbl['TRADEBOOK']['cash_amount_col']
        # Cash amount change
        cash_amount = latest_trade[cash_amount_col]

        # Currenct cash and ccy amounts
        current_ccy_amount = wallet[currency]
        current_cash_amount = wallet[CASH]

        # If result was buy
        if buy_sell_result == self._buy_sell_decision['BUY']:
            # How much cash is the hamster spending
            new_cash_amount = current_cash_amount - cash_amount
            # How much of the currency do we get
            new_ccy_amount = current_ccy_amount + ccy_amount
        
        # If result was sell
        elif buy_sell_result == self._buy_sell_decision['SELL']:    
            # How much ccy is the hamster selling
            new_ccy_amount = current_ccy_amount - ccy_amount
            # How much cash do we get
            new_cash_amount = current_cash_amount + cash_amount

        else:
            logmsg = f'Buy/sell result not understood, was {buy_sell_result}.'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            raise ValueError(logmsg)

        # Update CCY query
        qry_ccy = self.get_qry_update_wallet_ccy(new_ccy_amount=new_ccy_amount)
        # Update CASH query
        qry_cash = self.get_qry_update_wallet_cash(new_cash_amount=new_cash_amount)

        try:
            cursor = mysql_connection.cursor()
            if qry_ccy:
                cursor.execute(qry_ccy)
            if qry_cash:
                cursor.execute(qry_cash)
            mysql_connection.commit()   
            logmsg = f'Updated wallet with query: ' + qry_ccy + qry_cash
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update wallet with query: ' + qry_ccy + qry_cash
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )

    def get_qry_update_wallet_cash(
        self,
        new_cash_amount: float
    ) -> str:
        """Method to get the query to update the cash in the wallet.

        Args:
            new_cash_amount: New amount in cash.

        Returns
            Query to update wallet.
        """
        table = self._db_tbl['WALLET']['name']
        currency_symbol_col = self._db_tbl['WALLET']['currency_symbol_col']
        amount_col = self._db_tbl['WALLET']['amount_col']

        qry = f'UPDATE {table} ' +\
            f'SET ' +\
            f'{amount_col} = {new_cash_amount} ' +\
            f'WHERE {currency_symbol_col} = \"{CASH}\"'

        return qry

    def get_qry_update_wallet_ccy(
        self,
        new_ccy_amount: float,
        currency: str
    ) -> str:
        """Method to get the query to update the currency in the wallet.

        Args:
            new_ccy_amount: New amount in the currency.
            currency: Currency to update.

        Returns
            Query to update wallet.
        """
        table = self._db_tbl['WALLET']['name']
        currency_symbol_col = self._db_tbl['WALLET']['currency_symbol_col']
        amount_col = self._db_tbl['WALLET']['amount_col']

        # If we still have some currency left
        if new_ccy_amount > 0:
            qry = f'UPDATE {table} ' +\
                f'SET ' +\
                f'{amount_col} = {new_ccy_amount} ' +\
                f'WHERE {currency_symbol_col} = \"{currency}\"'
        # Hamster decided to sell all
        else:
            qry = f'DELETE FROM {table} ' +\
                f'WHERE {currency_symbol_col} = \"{currency}\"'

        return qry

    def update_wallet(
        self,
        mysql_connection: pymysql.connections.Connection,
        wallet: Dict[str, float],
        latest_trade: pd.core.series.Series
    ) -> None:
        """Method to update the wallet. Updates the database table and the wallet object.

        Args:
            mysql_connection: MySQL connection.
            wallet: Wallet of the hamster.
            latest_trade: Latest trade series.

        Returns:
            None.
        """
        # If currency is in the wallet, we need to update
        trdbk_currency_symbol_col = self._db_tbl['TRADEBOOK']['currency_symbol_col']
        currency = latest_trade[trdbk_currency_symbol_col]
        if currency in list(wallet.keys()):
            self.update_existing_currency(
                mysql_connection=mysql_connection,
                wallet=wallet,
                latest_trade=latest_trade
            )
        # Else the currency is new, we need to add new currency
        else:
            self.add_new_currency(
                mysql_connection=mysql_connection,
                wallet=wallet,
                latest_trade=latest_trade
            )

import pymysql
from datetime import datetime
from typing import Dict, Union
import pandas as pd

from utils import (
    get_latest_row_by_id,
    log
)

from constants import (
    DB_TBL,
    CRYPTOHAMSTER_LOG_FILE_PATH,
    PRINTOUT,
    BUY,
    SELL
)

from binance import Binance

class Tradebook:
    """Class to manage the tradebook.

    The tradebook keeps track of the cryptocurrencies bought/sold by the hamster.
    """
    
    def __init__(
        self,
        
        session_id: int,
        decision_cycle: int,
        buy_sell_result: str,
        currency: str,
        amount_percentage: float
        ) -> None:
        """Class instantiation.

        Args:
            mysql_connection: MySQL connection
            session_id: Active session id.
            decision_cycle: Active decision cycle.
            buy_sell_result: Result of the buy / sell decision.
            currency: Cryptocurrency to buy.
            amount_percentage: Percentage amount of the currency to buy or sell.
        """
        # Database tables
        self._db_tbl = DB_TBL
        # Decision options
        self._buy = BUY
        self._sell = SELL
        # Change in cash amount due to the trade
        self._cash_amount = 0
        # Change in ccy amount due to the trade
        self._ccy_amount = 0
        
        self._session_id = session_id
        self._decision_cycle = decision_cycle
        self._buy_sell_result = buy_sell_result
        self._currency = currency
        self._amount_percentage = float(amount_percentage)

    def get_latest_trade(
        self,
        mysql_connection: pymysql.connections.Connection,
        ) -> Union[None, pd.core.series.Series]:
        """Method to get the latest entry in the tradebook table. 

        Args:
            mysql_connection: MySQL connection

        Returns:
            Series with the latest tradebook. If there is no latest trade, returns None.
        """
        table = self._db_tbl['TRADEBOOK']['name']
        id_col = self._db_tbl['TRADEBOOK']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=mysql_connection,
            table=table,
            id_col=id_col
        )

        return s

    def process_trade(
        self,
        mysql_connection: pymysql.connections.Connection,
        wallet: Dict[str , float],
        ) -> None:
        """Method to process the trade.

        Args:
            mysql_connection: MySQL connection
            wallet: Wallet of the hamster.
        
        Raises:
            ValueError if the decision is not understood.

        Returns:
            None.
        """
        # Retrieve the price of the cryptocurrency, units of CCY/USD
        price = Binance(currency=self._currency).get_price()
        self._price = price

        # If buy
        if self._buy_sell_result == self._buy:
            # Check how much cash the hamster holds
            cash = wallet['CASH']
            # Cash amount to spend
            self._cash_amount = self._amount_percentage * cash
            # Amount the hamster can buy with that cash
            self._ccy_amount = self._cash_amount / price

        # if sell
        elif self._buy_sell_result == self._sell:
            # Get how much of the currency the hamster holds
            available_funds = wallet[self._currency]
            # Calculate amount to sell
            self._ccy_amount = self._amount_percentage * available_funds
            # Calculate how much cash will be added by the sale
            self._cash_amount = self._ccy_amount * price

        else:
            # If neither, something went wrong
            logmsg = f'Error: Buy sell result {self._buy_sell_result} not understood.'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            raise ValueError(logmsg)
        
        # Update tradebook
        self.update_tradebook(mysql_connection=mysql_connection)

    def update_tradebook(
        self,
        mysql_connection: pymysql.connections.Connection,
        ) -> None:
        """Method to update the tradebook. Updates the MySQL table.

        Args:
            mysql_connection: MySQL connection

        Returns:
            None.
        """
        table = self._db_tbl['TRADEBOOK']['name']
        session_id_col = self._db_tbl['TRADEBOOK']['session_id_col']
        decision_cycle_col = self._db_tbl['DECISION']['decision_cycle_col']
        buy_sell_col = self._db_tbl['TRADEBOOK']['buy_sell_col']
        currency_symbol_col = self._db_tbl['TRADEBOOK']['currency_symbol_col']
        cash_amount_col = self._db_tbl['TRADEBOOK']['cash_amount_col']
        ccy_amount_col = self._db_tbl['TRADEBOOK']['ccy_amount_col']
        time_col = self._db_tbl['TRADEBOOK']['time_col']
        ccy_price_col = self._db_tbl['TRADEBOOK']['ccy_price_col']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{session_id_col}, ' +\
              f'{decision_cycle_col}, ' +\
              f'{buy_sell_col}, ' +\
              f'{currency_symbol_col}, ' +\
              f'{cash_amount_col}, ' +\
              f'{ccy_amount_col}, ' +\
              f'{ccy_price_col}, ' +\
              f'{time_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'{self._session_id}, ' +\
              f'{self._decision_cycle}, ' +\
              f'\"{self._buy_sell_result}\", ' +\
              f'\"{self._currency}\", ' +\
              f'{self._cash_amount}, ' +\
              f'{self._ccy_amount}, ' +\
              f'{self._price}, ' +\
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



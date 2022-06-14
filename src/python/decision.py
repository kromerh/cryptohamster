from ast import Assert
from datetime import datetime
import numpy as np
import pymysql
import pandas as pd
from typing import Dict, Union


# Which types of decisions
DECISIONS = ['buy_sell', 'currency', 'amount']
# Options of amounts to buy/sell. Is in fraction of total. 100 is all in.
AMOUNT = np.arange(1, 101)
# MySQL tables
# Wallet: Contains what cryptocurrencices the hamster holds in its wallet
WALLET = 'wallet'

class Decision():
    """Class that contains objects to come to a decision using the hamsterwheel
    """
    
    def __init__(mysql_kwargs: Dict[str, str]) -> None:
        """Class instantiation.

        Args:
            mysql_kwargs: MySQL keyword arguments

        """
        # List of available decisions
        self._DECISIONS = DECISIONS
        # Which decision is currently being processed
        self._decision = None
        # Instantiate decision lists
        self._buy_sell = []
        self._currency = []
        self._AMOUNT = AMOUNT
        # MySQL tables
        self._WALLET = WALLET
        self._mysql_kwargs = Decision._validate_mysql_kwargs(mysql_kwargs)
    

    @classmethod
    def _validate_mysql_kwargs(self, mysql_kwargs: Dict[str, str]) -> Union[Dict[str, str], None]:
        """Method to validate the MySQL keyword arguments.

        Returns: mysql_kwargs if the dictionary passes the validation.

        Raises: ValueError if the expected keywords are not present in the mysql_kwargs
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

        

            # Create mysql connection
mysql_connection = pymysql.connect(
    host=HOST_REMOTE,
    user=user,
    password=password,
    db=DATABASE,
    port=PORT,
    charset='utf8'
)

    
    def connect_to_database(self) -> pymysql.connections.Connection:
        """Method to return the MySQL connection to the database.
        """
        mysql_connection = pymysql.connect(
            charset='utf8',
            **self._mysql_kwargs
        )


    def get_owned_currencies(self) -> pd.DataFrame:
        """Method to receive the currencies and amount that the hamster holds.

        Args:
            connection: MySQL connection.
        """
        qry = f'SELECT * FROM {self._WALLET}'
        df = pd.read_sql(
            sql=qry,
            con=mysql_connection,
            index_col='hamsterwheel_id'
        )


    def make_decision(self, counter: int) -> None:
        """Method to make a decision after the hamster stopped in the wheel

        Args:
            counter: Number that is used to define which item in the list

        """

    



    # @classmethod
    # def _validate_mysql_kwargs(self, mysql_kwargs: Dict[str, str]) -> Union[Dict[str, str], None]:
    #     """Method to validate the MySQL keyword arguments.

    #     Returns:
    #         mysql_kwargs if the dictionary passes the validation.

    #     Raises:
    #         ValueError if the expected keywords are not present in the mysql_kwargs
    #     """
    #     expected_keys = ['host', 'user', 'password', 'db', 'port']
    #     expected_keys.sort()
    #     check_list = list(mysql_kwargs.keys())
    #     check_list.sort()

    #     try:
    #         assert expected_keys == check_list
    #     except AssertionError:
    #         raise ValueError(f"User input invalid. Not all mysql keywords in the keyword list.")
        
    #     return mysql_kwargs

    
    # def connect_to_database(self) -> None:
    #     """Method to get the MySQL connection to the database.
    #     """
    #     self._mysql_connection = pymysql.connect(
    #         charset='utf8',
    #         **self._mysql_kwargs
    #     )


    # def get_wallet(self) -> pd.DataFrame:
    #     """Method to receive the currencies and amount that the hamster holds.

    #     Returns:    
    #         Dataframe with owned currencies and values.
    #     """
    #     wallet_table = self._db_tbl['WALLET']
    #     qry = f'SELECT * FROM {wallet_table}'
    #     df = pd.read_sql(
    #         sql=qry,
    #         con=self._mysql_connection,
    #         index_col='wallet_id'
    #     )

    #     return df


    


    # def get_decision_number(self):
    #     """Method to get the decision number.

    #     After a decision is reached, the number of turns of the wheel in the recent decision period is taken. With this
    #     number, the current decision list is evaluated to reach a decision.
    #     """


    # def go_to_next_decision(self):
    #     """Method to go to the next decision in the decision list.
    #     """
    #     # Go to the next decision key, if larger than the number of keys, go to key 1
    #     next_key = self._current_decision_number + 1
    #     if next_key >= len(self._current_decision_number.keys()):
    #         next_key = 1

    #     next_decision = self._decisions[str(next_key)]

    #     # Add to the log
    #     logmsg = f'Current key is {self._current_decision_number}, next key is {next_key}, next decision is ' +\
    #         f'{self._decisions[str(next_key)]}'
    #     log(
    #         log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
    #         logmsg=logmsg,
    #         printout=PRINTOUT
    #     )

    #     return next_decision
    

    # def set_next_decision(self, next_decision: str):
    #     """Method to set the next decision.
    #     """
    #     # Cash
    #     mask = self._wallet['CCY'] == 'CASH'
    #     cash = self._wallet.loc[mask, 'Balance'].values[0]
    #     # Number of currencies
    #     CCYs = self._wallet.loc[~mask].count()
    #     num_currencies = CCYs # all in the wallet except the cash

    #     # If the next decision is buy or sell
    #     if next_decision == self._decisions['1']:
    #         if (cash < THRESHOLD_CASH) & (num_currencies > 0):
    #             # If the hamster has no cash, the decision is sell
    #             logmsg = f'Hamster has no cash, cash is {cash}, decision is sell.'
    #             log(
    #                 log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
    #                 logmsg=logmsg,
    #                 printout=PRINTOUT
    #             )
    #             self._buy_sell = 'SELL'
    #             # Return the next decision
    #             next_decision = self.go_to_next_decision()
    #         elif num_currencies == 0:
    #             # If the hamster has no currencies, but some cash the decision is buy
    #             logmsg = f'Hamster has no currencies but cash, number of currencies are {num_currencies}, decision is buy.'
    #             log(
    #                 log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
    #                 logmsg=logmsg,
    #                 printout=PRINTOUT
    #             )
    #             self._buy_sell = 'BUY'
    #             # Return the next decision
    #             next_decision = self.go_to_next_decision()
    #         if (cash < THRESHOLD_CASH) & (num_currencies == 0):
    #             logmsg = f'Hamster is broke. Cash is {cash}, number of currencies are {num_currencies}.' +\
    #                 ' Hamster is done investing!'
    #             log(
    #                 log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
    #                 logmsg=logmsg,
    #                 printout=PRINTOUT
    #             )
    #             # Hamster is broke
    #             # Add to the decision table that the hamster is broke
    #             pass
        
    #     return next_decision


    # def make_decision(self, decision_number: int) -> None:
    #     """Method to make a decision after the hamster stopped in the wheel.

    #     Args:
    #         decision_number: Number that is used to define which item in the list to decide on.

    #     """
    #     # Get what the current decision is


    # def main(self):
    #     """Main method to run the decision wheel using the hamsterwheel!
    #     """
    #     # Read the current decision status from the database

    # # 
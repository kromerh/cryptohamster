# from matplotlib.style import available


# class Tradebook:
#     """Class to manage the tradebook.

#     The tradebook keeps track of the cryptocurrencies bought/sold by the hamster.
#     """
    
#     def __init__(
#         self,
#         mysql_connection: pymysql.connections.Connection,

#         ) -> None:
#         """Class instantiation.

#         Args:
#             mysql_connection: MySQL connection

#         """
#         # Database tables
#         self._db_tbl = DB_TBL
#         self._mysql_connection = mysql_connection
        

#     def calculate_amount(
#         self,
#         available_funds: float,
#         amount_percentage: float
#         ) -> float:
#         """Method to calculate the amount of a currency to buy or sell.

#         Args:
#             available_funds: Number that is USD either available cash (in buy scenario) or available
#                 amount in a currency.
#             amount_percentage: Percentage value that comes from the decision wheel.
        
#         Returns:
#             Amount to either buy or sell.
#         """
#         # Amount to buy or sell
#         amount = amount_percentage * available_funds

#         return amount


#     def buy_sell_currency(
#         self,
#         currency: str,
#         amount: float
#         ) -> None:
#         """Method to buy a cryptocurrency. Updates the MySQL table.

#         Args:
#             currency: Cryptocurrency to buy.
#             amount: Amount of the currency to buy.
        
#         Returns:
#             None.
#         """



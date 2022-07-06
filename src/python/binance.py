import numpy as np
import requests
import json
from typing import Optional, List
from utils import (
    log
)

from constants import (
    CRYPTOHAMSTER_LOG_FILE_PATH,
    PRINTOUT,
    BUY,
    SELL
)


class Binance():
    """Class to retrieve prices from binance.
    """

    def __init__(
        self,
        currency: Optional[str] = None
    ) -> None:
        """Class init.

        Args:
            currency: Currency symbol.

        Returns:
            None.
        """
        if currency:
            self._currency = currency
    
    def get_price(self) -> float:
        """Method to get the price of a cryptocurrency.

        Returns:
            Price in USD.
        """
        url = f'https://www.binance.com/api/v3/ticker/price?symbol={self._currency}'
        # requesting data from url
        data = requests.get(url)  
        data = data.json()

        price = float(data['price'])
            
        logmsg = f'Price for {self._currency} is {price}.'
        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return price

    def get_available_currencies(
        self,
        buy_sell_decision: str,
        hamsters_currencies: List[str]
    ) -> List[str]:
        """Method to return the list of available currencies to buy.

        Args:
            buy_sell_decision: Buy or sell decision.
            hamsters_currencies: Currencies the hamster holds.

        Returns:
            List of currency symbols from binance.
        """
        if buy_sell_decision == BUY:
            with open('./currency_symbol.txt', 'r') as f:
                lines = f.readlines()
                currencies = [l.strip() for l in lines]
                f.close()

            logmsg = f'Retrieved currency symbols {currencies} from binance for buying.'
        elif buy_sell_decision == SELL:
            currencies = hamsters_currencies

            logmsg = f'Retrieved currency symbols {currencies} from the wallet for selling.'
        else:
            logmsg = f'Error! Decision type {buy_sell_decision} not understood.'
            currencies = []

        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return currencies

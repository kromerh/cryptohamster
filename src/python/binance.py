import numpy as np
from typing import Optional, List
from utils import (
    log
)

from constants import (
    CRYPTOHAMSTER_LOG_FILE_PATH,
    PRINTOUT,
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

        price = np.round(np.abs(np.random.normal(0, 1, 1)[0]), 2)
            
        logmsg = f'Price for {self._currency} is {price}.'
        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return price

    def get_available_currencies(self) -> List[str]:
        """Method to return the list of available currencies to buy.

        Returns:
            List of currency symbols from binance.
        """
        currencies = ['btc', 'eth', 'wilson']
            
        logmsg = f'Retrieved currency symbols {currencies} from binance.'
        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return currencies

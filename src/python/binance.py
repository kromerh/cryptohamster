import numpy as np
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
        currency: str
    ) -> None:
        """Class init.

        Args:
            currency: Currency symbol.

        Returns:
            None.
        """
        self._ccy = currency
    
    def get_price(self) -> float:
        """Method to get the price of a cryptocurrency.

        Returns:
            Price in USD.
        """

        price = np.round(np.random(0, 1000), 2)
            
        logmsg = f'Price for {self._currency} is {price}.'
        log(
            log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
            logmsg=logmsg,
            printout=PRINTOUT
        )

        return price

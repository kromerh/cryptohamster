import pandas as pd
import pymysql

from constants import (
    DECISION_LOG_FILE_PATH,
    DB_TBL,
    PRINTOUT,
    NO_END_TIME,
)
from utils import get_latest_row_by_id

class Session():
    """Class to manage a session.

    A session beings when the hamster starts running in the wheel with the intend to decide between buying and selling.
    The session ends successfully when all decisions have been made and the trade was executed. The session can also
    be terminated if the hamster steps off the wheel after any one decision and does not step back on after some time-
    out.
    """
    
    def __init__(
        self,
        mysql_connection: pymysql.connections.Connection,

        ) -> None:
        """Class instantiation.

        Args:
            mysql_connection: MySQL connection

        """
        # Database tables
        self._db_tbl = DB_TBL
        self._mysql_connection = mysql_connection
        
    
    def get_latest_session(self) -> pd.core.series.Series:
        """Method to get the latest entry in the session table. 

        Returns:
            Series with the latest session.
        """
        table = self._db_tbl['SESSION']['table']
        id_col = self._db_tbl['SESSION']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=self._mysql_connection,
            table=table,
            id_col=id_col
        )

        return s
    

    def is_session_open(self, latest_session: pd.core.series.Series) -> bool:
        """Method to determine if a session is open.

        Returns:
            True if there is a running session, False otherwise.
        """
        if latest_session['end_time'] == NO_END_TIME:
            return False
        else:
            return True
import pandas as pd
import pymysql
from typing import Union, Dict
from datetime import datetime

from constants import (
    CRYPTOHAMSTER_LOG_FILE_PATH,
    DB_TBL,
    PRINTOUT,
    NO_END_TIME,
    THRESHOLD_SESSION_TIMEOUT
)

from utils import (
    get_latest_row_by_id,
    log,
)

class Session():
    """Class to manage a session.

    A session beings when the hamster starts running in the wheel with the intend to decide between buying and selling.
    The session ends successfully when all decisions have been made and the trade was executed. The session can also
    be terminated if the hamster steps off the wheel after any one decision and does not step back on after some time-
    out.
    """
    
    def __init__(
        self,
        ) -> None:
        """Class instantiation.
        """
        # Database tables
        self._db_tbl = DB_TBL
        
    
    def get_latest_session(
        self,
        mysql_connection: pymysql.connections.Connection,
        ) -> Union[None, pd.core.series.Series]:
        """Method to get the latest entry in the session table. 

        Args:
            mysql_connection: MySQL connection

        Returns:
            Series with the latest session. If there is no latest session, returns None.
        """
        table = self._db_tbl['SESSION']['name']
        id_col = self._db_tbl['SESSION']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=mysql_connection,
            table=table,
            id_col=id_col
        )

        return s


    def get_latest_hamsterwheel(
        self,
        mysql_connection: pymysql.connections.Connection,
        ) -> Union[None, pd.core.series.Series]:
        """Method to get the latest entry in the hamsterwheel table. 

        Args:
            mysql_connection: MySQL connection

        Returns:
            Series with the latest session. If there is no latest session, returns None.
        """
        table = self._db_tbl['HAMSTERWHEEL']['name']
        id_col = self._db_tbl['HAMSTERWHEEL']['id_col']

        s = get_latest_row_by_id(
            mysql_connection=mysql_connection,
            table=table,
            id_col=id_col
        )

        return s


    def start_new_session(
        self,
        mysql_connection: pymysql.connections.Connection,
        start_hamsterwheel_id: int
        ) -> None:
        """Method to start a new session. Updates the database.

        Args:
            start_hamsterwheel_id: Id of the hamsterwheel when the session started.
            mysql_connection: MySQL connection
        
        Returns:
            None.
        """ 
        # Database table and columns
        table = self._db_tbl['SESSION']['name']
        start_time_col = self._db_tbl['SESSION']['start_time_col']
        hamsterwheel_id_start_col = self._db_tbl['SESSION']['hamsterwheel_id_start_col']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'INSERT INTO {table} ' +\
              f'( ' +\
              f'{start_time_col}, ' +\
              f'{hamsterwheel_id_start_col} ' +\
              f') ' +\
              f'VALUES ' +\
              f'( ' +\
              f'\"{now}\", ' +\
              f'{start_hamsterwheel_id} ' +\
              f')'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()   
            logmsg = f'Started new session with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed insert new session with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
    

    def is_session_open(self, latest_session: pd.core.series.Series) -> bool:
        """Method to determine if a session is open.

        Args:
            latest_session: Latest session row.

        Returns:
            True if there is a running session, False otherwise.
        """
        end_time_col = self._db_tbl['SESSION']['end_time_col']
        if latest_session[end_time_col] == NO_END_TIME:
            return True
        else:
            return False


    def is_session_timeout(
        self,
        latest_session: pd.core.series.Series,
        threshold: int = THRESHOLD_SESSION_TIMEOUT
        ) -> bool:
        """Method to check if an open session is timed out.

        If the last turn of the wheel is more than 1 hour ago, the session is considered timed out and will be closed.

        Args:
            latest_session: Latest session row.
            threshold: Time in seconds after which the session is considered timed out.

        Returns:
            True if session is timeout, False otherwise.
        """
        start_time_col = self._db_tbl['SESSION']['start_time_col']
        id_col = self._db_tbl['SESSION']['id_col']
        # Time difference between last reading and now
        time_diff = (datetime.now() - latest_session[start_time_col]).total_seconds()

        if time_diff > threshold:
            # Session is timed out
            # Get the id
            latest_session_id = latest_session.name
            # Add to the log
            logmsg = f'Session timed out, time difference was {time_diff} seconds. Session id is {latest_session_id}.'
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
            return True
        
        return False


    def update_session_closed(
        self,
        latest_session: pd.core.series.Series,
        mysql_connection: pymysql.connections.Connection,
        end_type: str
        ) -> None:
        """Method to update the session and maark it as closed.

        Args:
            latest_session: Latest session row.
            mysql_connection: MySQL connection
            end_type: Reason for decision closing.
        
        Returns:
            None.
        """
        table = self._db_tbl['SESSION']['name']
        end_time_col = self._db_tbl['SESSION']['end_time_col']
        end_type_col = self._db_tbl['SESSION']['end_type_col']
        id_col = self._db_tbl['SESSION']['id_col']

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        qry = f'UPDATE {table} ' +\
              f'SET ' +\
              f'{end_time_col} = \"{now}\", ' +\
              f'{end_type_col} = \"{end_type}\" ' +\
              f'WHERE {id_col} = \"{latest_session.name}\"'
        try:
            cursor = mysql_connection.cursor()
            cursor.execute(qry)
            mysql_connection.commit()   
            logmsg = f'Updated session as closed with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )
        except Exception as e:
            print("Exeception occured:{}".format(e))
            logmsg = f'Failed update session as closed with query: ' + qry
            log(
                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                logmsg=logmsg,
                printout=PRINTOUT
            )

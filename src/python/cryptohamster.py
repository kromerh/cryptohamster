from numpy import full
from decision import Decision
from session import Session
import pandas as pd
from typing import List
import time
import os
import sys

# Pandas will warn us that we are using pymysql instead of SQLAlchemy, we are fine with that.
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

sys.path.append(os.path.dirname(__file__))


from constants import (
    BUY_SELL,
    TIMEOUT,
    CRYPTOHAMSTER_LOG_FILE_PATH,
    PRINTOUT,
    FULL_PATH_TO_CREDENTIALS
)

from utils import (
    create_mysql_connection,
    log,
)

# Session class
sess = Session()
# Decision
dec = Decision()

# Last hamsterwheel readings
hamsterwheel_readings = []

def is_hamster_running(
    latest_hamsterwheel: pd.core.series.Series,
    ) -> bool:
    """Function to determine if the hamster is running or not.
    
    Args:
        latest_hamsterwheel: Latest reading of the hamsterwheel id table.
    
    Returns:
        True if last three readings in the while Loop are different, False otherwise.
    """
    latest_id = latest_hamsterwheel.name

    if len(hamsterwheel_readings) == 3:
        # 3 readings acquired
        if (hamsterwheel_readings[0] < hamsterwheel_readings[1]) & (hamsterwheel_readings[1] < hamsterwheel_readings[2]):
            # Add the next reading
            hamsterwheel_readings.pop(0)
            hamsterwheel_readings.append(latest_id)
            return True
        else:
            hamsterwheel_readings.pop(0)
            hamsterwheel_readings.append(latest_id)
            return False
    elif len(hamsterwheel_readings) < 3:
        # Keep populating the list
        hamsterwheel_readings.append(latest_id)
        return False


# Loop every second
try:
    while True:
        # Initialize
        # MySQL connection with default args
        mysql_connection = create_mysql_connection(full_path_to_credentials=FULL_PATH_TO_CREDENTIALS)

        # Read the session table
        latest_session = sess.get_latest_session(mysql_connection=mysql_connection)
        # Read the latest hamsterwheel id
        latest_hamsterwheel = sess.get_latest_hamsterwheel(mysql_connection=mysql_connection)
        # Read the latest decision
        latest_decision = dec.get_latest_decision(mysql_connection=mysql_connection)

        # Check if the hamster is running. We use this trigger below.
        if is_hamster_running(latest_hamsterwheel=latest_hamsterwheel):
            # Hamster is running
            main_trigger = True
        else:
            # Hamster is not running
            main_trigger = False

        # If there is no session, initialize one
        if (latest_session is None):
            # Initialize only if the hamster is running
            if main_trigger:
                sess.start_new_session(
                    mysql_connection=mysql_connection,
                    start_hamsterwheel_id=latest_hamsterwheel.name
                )
                latest_session = sess.get_latest_session(mysql_connection=mysql_connection)
                # Start also a new decision, the starting one is always buy/sell
                _ = dec.start_new_decision(
                    mysql_connection=mysql_connection,
                    next_decision_type=BUY_SELL,
                    session_id=latest_session.name,
                    latest_hamsterwheel_id=latest_hamsterwheel.name
                )
            else:
                # Hamster is not running
                logmsg = f'Hamster is not running, no session active. Database has no session at all.'
                log(
                    log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                    logmsg=logmsg,
                    printout=PRINTOUT
                )

        # There is a session either open or closed
        else:
            # A session is running
            if sess.is_session_open(latest_session=latest_session):
                # Check if session timeout
                if sess.is_session_timeout(latest_session=latest_session):
                    # Session timed out
                    # Set session to close with end_type timeout
                    sess.update_session_closed(
                        mysql_connection=mysql_connection,
                        latest_session=latest_session,
                        end_type=TIMEOUT
                    )

                    # Close the open decision with timeout
                    dec.update_decision_closed(
                        mysql_connection=mysql_connection,
                        latest_decision=latest_decision,
                        latest_hamsterwheel=latest_hamsterwheel,
                        wheel_turns='NULL',
                        result=TIMEOUT
                        )

                # Session is running and not timed out yet
                else:
                    # Check if a decision is running
                    if latest_decision is None:
                        # There is no decision in the database
                        if main_trigger:
                            _ = dec.start_new_decision(
                                mysql_connection=mysql_connection,
                                next_decision_type=BUY_SELL,
                                session_id=latest_session.name,
                                latest_hamsterwheel_id=latest_hamsterwheel.name
                            )
                        # No decision in the database, but hamster is not running
                        # Note: This event can occur after the decision table was wiped.
                        else:
                            # Hamster is not running
                            logmsg = f'Hamster is not running, there is an active session, but no decision.'
                            log(
                                log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                                logmsg=logmsg,
                                printout=PRINTOUT
                            )                        
                    
                    # There is at least one decision entry in the database
                    else:
                        # There is a decision in the database, check if it is open or closed
                        if dec.is_decision_open(latest_decision=latest_decision):
                            # There is an open decision
                            # Check timeout
                            if dec.is_decision_timeout(
                                latest_decision=latest_decision,
                                latest_hamsterwheel=latest_hamsterwheel
                                ):
                                dec.update_decision_closed(
                                    mysql_connection=mysql_connection,
                                    latest_decision=latest_decision,
                                    latest_hamsterwheel=latest_hamsterwheel,
                                    wheel_turns='NULL',
                                    result=TIMEOUT
                                )
                            # Decision not timed out
                            else:
                                # Check if the decision is reached
                                if dec.is_decision_reached(latest_hamsterwheel=latest_hamsterwheel):
                                    # Decision has been reached
                                    logmsg = f'Decision has been reached.'
                                    log(
                                        log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                                        logmsg=logmsg,
                                        printout=PRINTOUT
                                    )
                                    num_wheelturns = dec.calculate_num_of_wheel_turns(
                                        mysql_connection=mysql_connection,
                                        latest_decision=latest_decision,
                                        latest_hamsterwheel=latest_hamsterwheel
                                    )
                                    # Get the result
                                    result = dec.determine_decision(
                                        latest_decision=latest_decision,
                                        num_wheelturns=num_wheelturns
                                    )
                                    dec.update_decision_closed(
                                        mysql_connection=mysql_connection,
                                        latest_decision=latest_decision,
                                        latest_hamsterwheel=latest_hamsterwheel,
                                        wheel_turns=num_wheelturns,
                                        result=result
                                    )

                                # Decision has not been reached yet
                                else:
                                    logmsg = f'Decision is active and not yet reached conclusion.'
                                    log(
                                        log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                                        logmsg=logmsg,
                                        printout=PRINTOUT
                                    )
                                
                            
                        # There is a decision in the database, and it is closed. No active decision
                        else:
                            # Check if the hamster is running
                            if main_trigger:
                                # Get the next decision
                                next_decision = dec.get_next_decision(latest_decision=latest_decision)
                                # Start a new decision
                                _ = dec.start_new_decision(
                                    mysql_connection=mysql_connection,
                                    next_decision_type=next_decision,
                                    session_id=latest_session.name,
                                    latest_hamsterwheel_id=latest_hamsterwheel.name)
                                logmsg = f'No decision active, hamster is running. Started new decision {next_decision}.'
                                log(
                                    log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                                    logmsg=logmsg,
                                    printout=PRINTOUT
                                )
                            else:
                                # Hamster is not running
                                logmsg = f'No decision active, hamster is not running. Doing nothing.'
                                log(
                                    log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                                    logmsg=logmsg,
                                    printout=PRINTOUT
                                )
            # There is no session running (and the database has a closed session)
            else:
                # Check if the hamster is running
                if main_trigger:
                    sess.start_new_session(
                        mysql_connection=mysql_connection,
                        start_hamsterwheel_id=latest_hamsterwheel.name
                    )
                    latest_session = sess.get_latest_session(mysql_connection=mysql_connection)
                    # Start also a new decision, the starting one is always buy/sell
                    _ = dec.start_new_decision(
                        mysql_connection=mysql_connection,
                        next_decision_type=BUY_SELL,
                        session_id=latest_session.name,
                        latest_hamsterwheel_id=latest_hamsterwheel.name
                    )
                # Hamster is not running
                else:
                    logmsg = f'Hamster is not running, no session active.'
                    log(
                        log_path=CRYPTOHAMSTER_LOG_FILE_PATH,
                        logmsg=logmsg,
                        printout=PRINTOUT
                    )
        time.sleep(1)
        mysql_connection.close()
except KeyboardInterrupt:
    mysql_connection.close()
finally:
    if mysql_connection.open:
        mysql_connection.close()

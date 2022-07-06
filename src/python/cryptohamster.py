from decision import Decision
from session import Session
from wallet import Wallet
from tradebook import Tradebook
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
    CURRENCY,
    AMOUNT,
    DB_TBL,
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
# Wallet
wallet = Wallet()

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
        # Read the wallet
        wallet = Wallet().get_wallet(mysql_connection=mysql_connection)
        # Update the decision options and get them
        dec.update_decision_options(wallet=wallet)
        decision_options = dec.get_decision_options()
        next_decision_type = decision_options[BUY_SELL]

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
                latest_decision = dec.get_latest_decision(mysql_connection=mysql_connection)
                # Start also a new decision, the starting one is always buy/sell unless the hamster is out of cash
                _ = dec.start_new_decision(
                    mysql_connection=mysql_connection,
                    next_decision_type=next_decision_type,
                    session_id=latest_session.name,
                    latest_hamsterwheel_id=latest_hamsterwheel.name,
                    latest_decision=latest_decision
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
                            latest_decision = dec.get_latest_decision(mysql_connection=mysql_connection)
                            _ = dec.start_new_decision(
                                mysql_connection=mysql_connection,
                                next_decision_type=next_decision_type,
                                session_id=latest_session.name,
                                latest_hamsterwheel_id=latest_hamsterwheel.name,
                                latest_decision=latest_decision
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
                                    # Read the wallet again
                                    wallet = Wallet().get_wallet(mysql_connection=mysql_connection)
                                    # Get the result
                                    result = dec.determine_decision(
                                        latest_decision=latest_decision,
                                        num_wheelturns=num_wheelturns,
                                        wallet=wallet
                                    )
                                    dec.update_decision_closed(
                                        mysql_connection=mysql_connection,
                                        latest_decision=latest_decision,
                                        latest_hamsterwheel=latest_hamsterwheel,
                                        wheel_turns=num_wheelturns,
                                        result=result
                                    )
                                    # Check if the trade can be kicked off because all decisions are there
                                    # Retrieve the past 3 decisions from the decisions table
                                    past_decisions = dec.get_latest_decision(
                                        mysql_connection=mysql_connection,
                                        num_rows=3
                                    )
                                    if dec.check_all_decisions_for_trade(
                                        past_decisions=past_decisions,
                                        mysql_connection=mysql_connection
                                        ):
                                        
                                        type_col = DB_TBL['DECISION']['type_col']
                                        result_col = DB_TBL['DECISION']['result_col']
                                        decision_cycle_col = DB_TBL['DECISION']['decision_cycle_col']
                                        
                                        # Buy sell result
                                        m_buy_sell = past_decisions[type_col] == BUY_SELL
                                        buy_sell_result = past_decisions.loc[m_buy_sell, :].loc[:, result_col].values[0]
                                        # Currency result
                                        m_currency = past_decisions[type_col] == CURRENCY
                                        currency_result = past_decisions.loc[m_currency, :].loc[:, result_col].values[0]
                                        # Amount result
                                        m_amount = past_decisions[type_col] == AMOUNT
                                        amount_result = past_decisions.loc[m_amount, :].loc[:, result_col].values[0]
                                        
                                        tradebook = Tradebook(
                                            session_id=latest_session.name,
                                            decision_cycle=latest_decision[decision_cycle_col],
                                            buy_sell_result=buy_sell_result,
                                            currency=currency_result,
                                            amount_percentage=amount_result
                                        )

                                        # Read the wallet again
                                        wallet = Wallet().get_wallet(mysql_connection=mysql_connection)
                                        
                                        # process the trade
                                        tradebook.process_trade(
                                            mysql_connection=mysql_connection,
                                            wallet=wallet
                                        )

                                        # Update the wallet with the trade
                                        latest_trade = tradebook.get_latest_trade(mysql_connection=mysql_connection)
                                        Wallet().update_wallet(
                                            mysql_connection=mysql_connection,
                                            wallet=wallet,
                                            latest_trade=latest_trade
                                        )

                                    
                                    # Not all decisions were reached, continue
                                    else:
                                        pass
                                    


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
                                latest_decision = dec.get_latest_decision(mysql_connection=mysql_connection)
                                # Start a new decision
                                _ = dec.start_new_decision(
                                    mysql_connection=mysql_connection,
                                    next_decision_type=next_decision,
                                    session_id=latest_session.name,
                                    latest_hamsterwheel_id=latest_hamsterwheel.name,
                                    latest_decision=latest_decision
                                )
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
                    latest_decision = dec.get_latest_decision(mysql_connection=mysql_connection)
                    # Start also a new decision, the starting one is always buy/sell
                    _ = dec.start_new_decision(
                        mysql_connection=mysql_connection,
                        next_decision_type=next_decision_type,
                        session_id=latest_session.name,
                        latest_hamsterwheel_id=latest_hamsterwheel.name,
                        latest_decision=latest_decision
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

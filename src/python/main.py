import time
import pandas as pd
import logging
from typing import List, Optional
from datetime import datetime
import sys

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

DECISION_LIST = ["buy_or_sell", "currency", "amount"]
SLEEP_TIME = 0.5

mockup_raw_hamsterwheel_df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "time": [
            "2023-08-03 12:00:00",
            "2023-08-03 12:00:01",
            "2023-08-03 12:00:02",
            "2023-08-03 12:00:03",
            "2023-08-03 12:00:04",
            "2023-08-03 12:00:05",
            "2023-08-03 12:00:06",
            "2023-08-03 12:00:07",
            "2023-08-03 12:00:08",
            "2023-08-03 12:00:09",
        ],
        "magnet": [1, 0, 1, 0, 1, 0, 1, 0, 1, 1],
    }
)

mockup_latest_hamsterwheel_df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "time": [
            "2023-08-03 12:00:00",
            "2023-08-03 12:00:01",
            "2023-08-03 12:00:02",
            "2023-08-03 12:00:03",
            "2023-08-03 12:00:04",
        ],
        "magnet": [1, 1, 1, 1, 1],
    }
)

mockup_decision_df = pd.DataFrame(
    {
        "id": [1],
        "start_time": ["2023-08-03 12:00:00"],
        "end_time": [None],
        "type": ["buy_or_sell"],
        "number_hamsterwheel_turns": [None],
        "status": ["open"],
        "result": [None],
    }
)

mockup_wallet_df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "currency": ["BTC", "ETH", "DOGE", "USD"],
        "amount": [10, 4, 5, 10000],
    }
)

mockup_price_df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "currency": ["BTC", "ETH", "DOGE", "ADA", "AVAX", "XRP"],
        "price": [25_000, 1_595, 0.064, 0.25, 12.48, 0.5],
    }
)

# TODO Write function that executes orders
# TODO Write function to read price from binance and save to database


def get_latest_decision(df_decision: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Function to get the latest decision"""
    if df_decision is not None:
        # order by start_time desc
        df_decision = df_decision.sort_values(by="start_time", ascending=False)
        df_decision_latest = df_decision.iloc[[0]]
        return df_decision_latest
    return None


def is_hamsterwheel_running(df: pd.DataFrame) -> bool:
    """Function to determine if the hamsterwheel is running, i.e., there has been a magnet 0 in the last 5 seconds"""
    if 0 in df["magnet"].values:
        return True
    return False


def is_decision_open(df_decision_latest: pd.DataFrame) -> bool:
    """Function to determine if there is currently a decision open"""
    # During init, the length of df is 0
    if df_decision_latest is not None:
        if "open" in df_decision_latest["status"].values:
            return True
    return False


def get_next_decision(
    df_decision_latest: pd.DataFrame, decisions: List[str] = DECISION_LIST
) -> str:
    """Function to get the next decision"""
    if df_decision_latest is not None:
        current_decision = df_decision_latest["type"].values[0]
        current_decision_index = decisions.index(current_decision)
        next_decision_index = current_decision_index + 1
        if next_decision_index >= len(decisions):
            next_decision_index = 0
        return decisions[next_decision_index]

    # If there is no decision yet, return the first decision
    return decisions[0]


def save_new_decision(next_decision: str) -> None:
    """Function to save a new decision to the database"""
    save_log(message=f"Saving new decision: {next_decision}")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    new_decision = pd.DataFrame(
        {
            "start_time": [now],
            "end_time": [None],
            "number_hamsterwheel_turns": [None],
            "type": [next_decision],
            "status": ["open"],
            "result": [None],
        }
    )
    # TODO: Save the new decision to the database
    logger.debug(f"New decision: {new_decision}")


def update_decision(df_decision_latest: pd.DataFrame) -> None:
    """Function to update a decision in the database"""
    # TODO: Update the decision in the database
    save_log(message=f"Updating decision: {df_decision_latest}")


def get_decision_cycle(df_decision: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Function to get the decision cycle, i.e., all
    decisions after the last buy_or_sell decision"""
    # Get the last buy_or_sell decision
    if df_decision is not None:
        buy_or_sell_filter = df_decision["type"] == "buy_or_sell"
        df_buy_or_sell = df_decision[buy_or_sell_filter]
        # Order by start_time desc
        df_buy_or_sell = df_buy_or_sell.sort_values(by="start_time", ascending=False)
        last_buy_or_sell = df_buy_or_sell.iloc[[0]]
        last_buy_or_sell_index = last_buy_or_sell.index[0]
        # Get all the decisions after the last buy_or_sell decision
        df_decision_cycle = df_decision.iloc[last_buy_or_sell_index:]
        return df_decision_cycle
    return None


def has_money(df_wallet: pd.DataFrame) -> bool:
    """Function to check if the hamster has money.
    If the hamster has less than 5 USD, then it is broke"""
    if df_wallet is not None:
        if "USD" in df_wallet["currency"].values:
            usd_filter = df_wallet["currency"] == "USD"
            df_usd = df_wallet[usd_filter]
            if df_usd["amount"].values[0] > 5:
                return True
            else:
                return False
    return False


def has_crypto(df_wallet: pd.DataFrame) -> bool:
    """Function to determine if the hamster has crypto (True)."""
    if df_wallet is not None:
        # Check if there is any non USD currency
        currencies = df_wallet["currency"].values
        if len(currencies) > 1:
            return True
    return False


def get_buy_or_sell_decision(df_decision_cycle: pd.DataFrame) -> str:
    """Function to get the buy_or_sell decision"""
    # Return the result of the last buy_or_sell decision
    buy_or_sell_filter = df_decision_cycle["type"] == "buy_or_sell"
    df_buy_or_sell = df_decision_cycle[buy_or_sell_filter]
    return df_buy_or_sell["result"].values[0]


def get_currency_decision(df_decision_cycle: pd.DataFrame) -> str:
    """Function to get the currency decision"""
    # Return the result of the last currency decision
    currency_filter = df_decision_cycle["type"] == "currency"
    df_currency = df_decision_cycle[currency_filter]
    return df_currency["result"].values[0]


def get_currencies_to_sell(df_wallet: pd.DataFrame) -> List[str]:
    """Function to get the currencies to sell"""
    currencies_to_sell = []
    if df_wallet is not None:
        # Get all the currencies
        currencies = df_wallet["currency"].values
        # Remove USD from the currencies
        currencies = [currency for currency in currencies if currency != "USD"]
        # If there are currencies left, return them
        if len(currencies) > 0:
            return currencies
    return currencies_to_sell


def close_decision(
    df_decision_latest: pd.DataFrame,
    df_decision_cycle: pd.DataFrame,
    number_hamsterwheel_turns: int,
    end_time: datetime,
    df_wallet: pd.DataFrame,
) -> pd.DataFrame:
    """Function to close a decision and return the result"""
    save_log(message=f"Closing decision: {df_decision_latest}")

    df_decision_latest["end_time"] = end_time
    df_decision_latest["status"] = "closed"
    df_decision_latest["number_hamsterwheel_turns"] = number_hamsterwheel_turns
    current_decision = df_decision_latest["type"].values[0]
    if current_decision == "buy_or_sell":
        result = get_result_buy_or_sell(number_hamsterwheel_turns)
        # If the hamster has money but no crypto, then the result will be buy
        if has_money(df_wallet) and not has_crypto(df_wallet):
            result = "buy"

        # If the hamster has no money but crypto, then the result will be sell
        if not has_money(df_wallet) and has_crypto(df_wallet):
            result = "sell"

    elif current_decision == "currency":
        # TODO Add logic if the buy_or_sell decision was sell, then the currency_list is the list of currencies the hamster has
        if get_buy_or_sell_decision(df_decision_cycle) == "sell":
            currency_list = get_currencies_to_sell(df_wallet=df_wallet)
        if get_buy_or_sell_decision(df_decision_cycle) == "buy":
            currency_list = ["USD"]

        result = get_result_currency(
            number_hamsterwheel_turns=number_hamsterwheel_turns,
            currency_list=currency_list,
        )
    elif current_decision == "amount":
        result = get_result_amount(number_hamsterwheel_turns)
    df_decision_latest["result"] = result

    update_decision(df_decision_latest)

    return df_decision_latest


def get_number_hamsterwheel_turns(
    df_raw_hamsterwheel: pd.DataFrame, start_time: datetime, end_time: datetime
) -> int:
    """Function to get the number of hamsterwheel turns"""
    time_filter = (df_raw_hamsterwheel["time"] >= start_time) & (
        df_raw_hamsterwheel["time"] <= end_time
    )
    df_hamsterwheel_turns = df_raw_hamsterwheel[time_filter]
    magnet_filter = df_hamsterwheel_turns["magnet"] == 0
    df_hamsterwheel_turns = df_hamsterwheel_turns[magnet_filter]
    number_hamsterwheel_turns = len(df_hamsterwheel_turns)
    logger.debug(
        f"Between {start_time} and {end_time} there were {number_hamsterwheel_turns} hamsterwheel turns"
    )
    return number_hamsterwheel_turns


def get_result_buy_or_sell(number_hamsterwheel_turns: int) -> str:
    """Function to get the result of the buy or sell decision"""
    # If the number of hamsterwheel turns is even, then buy
    if number_hamsterwheel_turns % 2 == 0:
        return "buy"
    # If the number of hamsterwheel turns is odd, then sell
    return "sell"


def get_result_currency(
    number_hamsterwheel_turns: int, currency_list: List[str]
) -> str:
    """Function to get the result of the currency decision"""
    # Repeat the list of currencies until it is longer than the number of hamsterwheel turns
    currency_list = currency_list * (
        number_hamsterwheel_turns // len(currency_list) + 1
    )
    # Get the currency at the index of the number of hamsterwheel turns
    return currency_list[number_hamsterwheel_turns]


def get_result_amount(number_hamsterwheel_turns: int) -> float:
    """Function to get the result of the amount decision"""
    amount_options = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    # Repeat the list of amount options until it is longer than the number of hamsterwheel turns
    amount_options = amount_options * (
        number_hamsterwheel_turns // len(amount_options) + 1
    )
    # Get the amount at the index of the number of hamsterwheel turns
    return amount_options[number_hamsterwheel_turns]


def get_wallet_id(currency: str, df_wallet: pd.DataFrame) -> int:
    """Function to get the wallet id"""
    currency_filter = df_wallet["currency"] == currency
    df_currency = df_wallet[currency_filter]
    return df_currency["id"].values[0]


def get_price(currency: str, df_price: pd.DataFrame) -> float:
    """Function to get the price"""
    currency_filter = df_price["currency"] == currency
    df_currency = df_price[currency_filter]
    return df_currency["price"].values[0]


def prepare_order(
    df_decision_closed: pd.DataFrame,
    df_decision_cycle: pd.DataFrame,
    df_wallet: pd.DataFrame,
) -> pd.DataFrame:
    """Function to prepare the order"""
    # Get the buy_or_sell decision
    buy_or_sell_decision = get_buy_or_sell_decision(df_decision_cycle=df_decision_cycle)
    # Get the currency decision
    currency_decision = get_currency_decision(df_decision_cycle=df_decision_cycle)
    # Get the amount decision
    amount_decision = df_decision_closed["result"].values[0]

    wallet_id = get_wallet_id(currency=currency_decision, df_wallet=df_wallet)
    price = get_price(currency=currency_decision, df_price=df_price)
    order = pd.DataFrame(
        {
            "wallet_id": [wallet_id],
            "type": [buy_or_sell_decision],
            "currency": [currency_decision],
            "amount": [amount_decision],
            "price": [price],
            "state": ["pending"],
        }
    )
    return order


def save_log(message: str) -> None:
    """Function to save a log message to the database"""
    logger.info(message)
    # TODO: Save the log message to the database
    # logger.info("Saved log message to database")


if __name__ == "__main__":
    while True:
        # TODO Check if there is an open order
        # TODO: Read the latest 5 seconds of hamsterwheel data from the mysql database and store in a pandas dataframe
        df_latest_hamsterwheel = mockup_latest_hamsterwheel_df

        # TODO: Read the latest decision in the decision table from the database
        df_decision = mockup_decision_df

        # TODO: Read the wallet of the hamster from the database
        df_wallet = mockup_wallet_df

        # TODO: Read the price of the currencies from the database
        df_price = mockup_price_df

        df_decision_latest = get_latest_decision(df_decision=df_decision)

        logger.debug(f"Latest decision: {df_decision_latest}")

        # Determine if the hamsterwheel is running, i.e., there has been a magnet 0 in the last 5 seconds
        is_running = is_hamsterwheel_running(df_latest_hamsterwheel)
        save_log(message=f"Hamsterwheel is running? {is_running}")

        # Check if there is currently a decision open
        decision_open = is_decision_open(df_decision_latest)
        save_log(message=f"Decision open? {decision_open}")

        # If the hamster has no money and no crypto, then it is broke
        if not has_money(df_wallet) and not has_crypto(df_wallet):
            save_log(message="Hamster is broke")
            sys.exit()
        # If the hamsterwheel is running and there is no decision open, then open a new decision
        if is_running and not decision_open:
            save_log(message="Opening a new decision")
            next_decision = get_next_decision(df_decision_latest)
            save_log(message=f"Next decision: {next_decision}")

            save_new_decision(next_decision)
            time.sleep(SLEEP_TIME)
            continue

        # If the hamsterwheel is not running and there is no decision open, then do nothing
        if not is_running and not decision_open:
            save_log(message="No decision open and hamsterwheel not running")
            time.sleep(SLEEP_TIME)
            continue

        # If the hamsterwheel is running and there is a decision open, then do nothing
        if is_running and decision_open:
            save_log(message="Hamsterwheel running and decision open")
            time.sleep(SLEEP_TIME)
            continue

        # If the hamsterwheel is not running and there is a decision open, then close the decision
        if not is_running and decision_open:
            save_log(message="Closing the decision")
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            # Check how many hamsterwheel turns there were
            number_hamsterwheel_turns = get_number_hamsterwheel_turns(
                df_raw_hamsterwheel=mockup_raw_hamsterwheel_df,
                start_time=df_decision_latest["start_time"].values[0],
                end_time=now,
            )
            logging.debug(f"Number of hamsterwheel turns: {number_hamsterwheel_turns}")
            # Get all the decisions for this cycle of decisions
            df_decision_cycle = get_decision_cycle(df_decision=df_decision)
            df_decision_closed = close_decision(
                df_decision_latest=df_decision_latest,
                df_decision_cycle=df_decision_cycle,
                number_hamsterwheel_turns=number_hamsterwheel_turns,
                end_time=now,
                df_wallet=df_wallet,
            )
            save_log(message=f"Decision closed: {df_decision_closed}")

            # If the decision was amount and is closed, place the order
            if df_decision_closed["type"].values[0] == "amount":
                save_log(message="Preparing order")
                order = prepare_order(
                    df_decision_closed=df_decision_closed,
                    df_decision_cycle=df_decision_cycle,
                    df_wallet=df_wallet,
                )
                logging.debug(f"Order: {order}")

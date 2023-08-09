from main import (
    is_hamsterwheel_running,
    get_number_hamsterwheel_turns,
    get_next_decision,
    get_result_buy_or_sell,
    get_result_currency,
    get_result_amount,
    close_decision,
    get_latest_decision,
    get_decision_cycle,
    has_money,
    has_crypto,
    get_buy_or_sell_decision,
    get_currencies_to_sell,
    get_currency_decision,
    get_wallet_id,
    get_price,
)
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


def test_is_hamsterwheel_running():
    """Function to test is_hamsterwheel_running()"""
    test_df = pd.DataFrame({"magnet": [1, 1, 1, 1, 0]})
    assert is_hamsterwheel_running(test_df) == True


def test_is_hamsterwheel_running_false():
    """Function to test is_hamsterwheel_running()"""
    test_df = pd.DataFrame({"magnet": [1, 1, 1, 1, 1]})
    assert is_hamsterwheel_running(test_df) == False


def test_get_number_hamsterwheel_turns():
    """Function to test get_number_hamsterwheel_turns()"""
    start_time = pd.Timestamp("2023-08-03 00:00:03")
    end_time = pd.Timestamp("2023-08-03 00:00:06")
    test_df = pd.DataFrame(
        {"time": pd.date_range("2023-08-03 00:00:00", "2023-08-03 00:00:07", freq="1s")}
    )
    test_df["magnet"] = [1, 1, 0, 1, 0, 1, 0, 0]
    print(test_df)
    assert (
        get_number_hamsterwheel_turns(
            df_raw_hamsterwheel=test_df, start_time=start_time, end_time=end_time
        )
        == 2
    )


def test_get_result_buy_or_sell():
    """Function to test get_result_buy_or_sell()"""
    number_hamsterwheel_turns = 66
    assert get_result_buy_or_sell(number_hamsterwheel_turns) == "buy"

    number_hamsterwheel_turns = 65
    assert get_result_buy_or_sell(number_hamsterwheel_turns) == "sell"

    number_hamsterwheel_turns = 0
    assert get_result_buy_or_sell(number_hamsterwheel_turns) == "buy"


def test_get_result_currency():
    """Function to test get_result_currency()"""
    currency_list = ["BTC", "ETH", "DOGE"]
    number_hamsterwheel_turns = 2
    assert (
        get_result_currency(
            number_hamsterwheel_turns=number_hamsterwheel_turns,
            currency_list=currency_list,
        )
        == "DOGE"
    )
    number_hamsterwheel_turns = 0
    assert (
        get_result_currency(
            number_hamsterwheel_turns=number_hamsterwheel_turns,
            currency_list=currency_list,
        )
        == "BTC"
    )
    number_hamsterwheel_turns = 3
    assert (
        get_result_currency(
            number_hamsterwheel_turns=number_hamsterwheel_turns,
            currency_list=currency_list,
        )
        == "BTC"
    )
    number_hamsterwheel_turns = 10
    assert (
        get_result_currency(
            number_hamsterwheel_turns=number_hamsterwheel_turns,
            currency_list=currency_list,
        )
        == "ETH"
    )


def test_get_result_amount():
    """Function to test get_result_amount()"""
    number_hamsterwheel_turns = 2
    assert get_result_amount(number_hamsterwheel_turns) == 0.3
    number_hamsterwheel_turns = 0
    assert get_result_amount(number_hamsterwheel_turns) == 0.1
    number_hamsterwheel_turns = 3
    assert get_result_amount(number_hamsterwheel_turns) == 0.4
    number_hamsterwheel_turns = 10
    assert get_result_amount(number_hamsterwheel_turns) == 0.1


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("buy_or_sell", "amount"),
        ("amount", "currency"),
        ("currency", "buy_or_sell"),
    ],
)
def test_get_next_decision(test_input, expected):
    """Function to test get_next_decision()"""
    df_decision_latest = pd.DataFrame(
        {
            "some_col": [None],
            "type": [test_input],
        }
    )
    decisions = ["buy_or_sell", "amount", "currency"]
    next_decision = get_next_decision(
        df_decision_latest=df_decision_latest, decisions=decisions
    )
    print(next_decision)
    assert next_decision == expected


def test_get_next_decision_none():
    """Function to test get_next_decision() if df_decision_latest is None"""
    df_decision_latest = None
    decisions = ["buy_or_sell", "amount", "currency"]
    next_decision = get_next_decision(
        df_decision_latest=df_decision_latest, decisions=decisions
    )
    print(next_decision)
    assert next_decision == "buy_or_sell"


def test_get_latest_decision():
    """Function to test get_latest_decision()"""
    df_decision = pd.DataFrame(
        {
            "some_col": [None, None, None],
            "type": ["buy_or_sell", "amount", "currency"],
            "start_time": [
                pd.Timestamp("2023-08-03 00:00:00"),
                pd.Timestamp("2023-08-04 00:00:03"),
                pd.Timestamp("2023-08-02 00:00:06"),
            ],
        }
    )
    latest_decision = get_latest_decision(df_decision=df_decision)
    expected_df = pd.DataFrame(
        {
            "some_col": [None],
            "type": ["amount"],
            "start_time": [pd.Timestamp("2023-08-04 00:00:03")],
        }
    )
    assert_frame_equal(
        latest_decision.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_get_latest_decision_none():
    """Function to test get_latest_decision() if df_decision is None"""
    df_decision = None
    latest_decision = get_latest_decision(df_decision=df_decision)
    assert latest_decision == None


def test_get_decision_cycle():
    """Function to test get_decision_cycle()"""
    df_decision = pd.DataFrame(
        {
            "some_col": ["A", "CCC", "BBB"],
            "type": ["currency", "buy_or_sell", "amount"],
            "start_time": [
                pd.Timestamp("2023-08-01 00:00:00"),
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    decision_cycle = get_decision_cycle(df_decision=df_decision)
    expected_df = pd.DataFrame(
        {
            "some_col": ["CCC", "BBB"],
            "type": ["buy_or_sell", "amount"],
            "start_time": [
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    assert_frame_equal(
        decision_cycle.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


def test_get_decision_cycle_none():
    """Function to test get_decision_cycle() if df_decision is None"""
    df_decision = None
    decision_cycle = get_decision_cycle(df_decision=df_decision)
    assert decision_cycle == None


def test_get_decision_cycle_more_rows():
    """Function to test get_decision_cycle() if df_decision has more rows"""
    df_decision = pd.DataFrame(
        {
            "type": [
                "buy_or_sell",
                "currency",
                "buy_or_sell",
                "amount",
                "currency",
                "buy_or_sell",
                "amount",
            ],
            "start_time": [
                pd.Timestamp("2023-08-01 00:00:00"),
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
                pd.Timestamp("2023-08-04 00:00:09"),
                pd.Timestamp("2023-08-05 00:00:12"),
                pd.Timestamp("2023-08-06 00:00:15"),
                pd.Timestamp("2023-08-07 00:00:18"),
            ],
        }
    )
    decision_cycle = get_decision_cycle(df_decision=df_decision)
    expected_df = pd.DataFrame(
        {
            "type": ["buy_or_sell", "amount"],
            "start_time": [
                pd.Timestamp("2023-08-06 00:00:15"),
                pd.Timestamp("2023-08-07 00:00:18"),
            ],
        }
    )
    assert_frame_equal(
        decision_cycle.reset_index(drop=True), expected_df.reset_index(drop=True)
    )


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (100, True),
        (0, False),
        (1, False),
        (99, True),
        (5, False),
        (5.1, True),
        (4.9, False),
    ],
)
def test_has_money(test_input, expected):
    """Function to test is_broke()"""
    df_wallet = pd.DataFrame(
        {
            "currency": ["BTC", "ETH", "DOGE", "USD"],
            "amount": [100, 2, 44, test_input],
        }
    )
    print(df_wallet)
    assert has_money(df_wallet=df_wallet) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (pd.DataFrame({"currency": ["BTC", "USD"], "amount": [100, 2]}), True),
        (
            pd.DataFrame({"currency": ["BTC", "USD", "ETH"], "amount": [1, 14, 55]}),
            True,
        ),
        (pd.DataFrame({"currency": ["USD"], "amount": [1]}), False),
        (pd.DataFrame({"currency": ["Haha"], "amount": [1]}), False),
    ],
)
def test_has_crypto(test_input, expected):
    """Function to test is_broke()"""
    assert has_crypto(df_wallet=test_input) == expected


def test_get_buy_or_sell_decision():
    """Function to test get_buy_or_sell_decision()"""
    df_decision_cycle = pd.DataFrame(
        {
            "some_col": ["A", "CCC", "BBB"],
            "type": ["currency", "buy_or_sell", "amount"],
            "result": ["BTC", "buy", 0.1],
            "start_time": [
                pd.Timestamp("2023-08-01 00:00:00"),
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    buy_or_sell_decision = get_buy_or_sell_decision(df_decision_cycle=df_decision_cycle)
    assert buy_or_sell_decision == "buy"


def test_get_buy_or_sell_decision_2():
    """Function to test get_buy_or_sell_decision()"""
    df_decision_cycle = pd.DataFrame(
        {
            "some_col": ["A", "CCC", "BBB"],
            "type": ["currency", "buy_or_sell", "amount"],
            "result": ["BTC", "sell", 0.1],
            "start_time": [
                pd.Timestamp("2023-08-01 00:00:00"),
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    buy_or_sell_decision = get_buy_or_sell_decision(df_decision_cycle=df_decision_cycle)
    assert buy_or_sell_decision == "sell"


def test_get_currencies_to_sell():
    """Function to test get_currencies_to_sell()"""
    df_wallet = pd.DataFrame(
        {
            "currency": ["BTC", "ETH", "DOGE", "USD"],
            "amount": [100, 2, 44, 0],
        }
    )
    expected_currencies_to_sell = ["BTC", "ETH", "DOGE"]
    assert get_currencies_to_sell(df_wallet=df_wallet) == expected_currencies_to_sell


def test_close_decision():
    """Function to test close_decision()"""
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    df_decision_latest = pd.DataFrame(
        {
            "some_col": ["BBB"],
            "type": ["amount"],
            "result": [None],
            "start_time": [
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    df_decision_cycle = pd.DataFrame(
        {
            "some_col": ["A", "CCC", "BBB"],
            "type": ["currency", "buy_or_sell", "amount"],
            "result": ["BTC", "buy", 0.1],
            "start_time": [
                pd.Timestamp("2023-08-01 00:00:00"),
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    df_wallet = pd.DataFrame(
        {
            "currency": ["BTC", "ETH", "DOGE", "USD"],
            "amount": [100, 2, 44, 0],
        }
    )
    end_time = pd.Timestamp("2023-08-04 00:05:09")
    df_decision_closed = close_decision(
        df_decision_latest=df_decision_latest,
        df_decision_cycle=df_decision_cycle,
        number_hamsterwheel_turns=2,
        end_time=end_time,
        df_wallet=df_wallet,
    )
    print(df_decision_closed)
    expected_df_decision_closed = pd.DataFrame(
        {
            "some_col": ["BBB"],
            "type": ["amount"],
            "result": [0.3],
            "start_time": [
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
            "end_time": [end_time],
            "status": ["closed"],
            "number_hamsterwheel_turns": [2],
        }
    )
    assert_frame_equal(
        df_decision_closed.reset_index(drop=True),
        expected_df_decision_closed.reset_index(drop=True),
    )


def test_get_currency_decision():
    """Function to test get_currency_decision()"""
    df_decision_cycle = pd.DataFrame(
        {
            "some_col": ["A", "CCC", "BBB"],
            "type": ["currency", "buy_or_sell", "amount"],
            "result": ["BTC", "buy", 0.1],
            "start_time": [
                pd.Timestamp("2023-08-01 00:00:00"),
                pd.Timestamp("2023-08-02 00:00:03"),
                pd.Timestamp("2023-08-03 00:00:06"),
            ],
        }
    )
    currency_decision = get_currency_decision(df_decision_cycle=df_decision_cycle)
    assert currency_decision == "BTC"


def test_get_wallet_id():
    """Function to test get_wallet_id()"""
    currency = "ABC"
    df_wallet = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "currency": ["BTC", "ETH", "ABC", "USD"],
            "amount": [100, 2, 44, 0],
        }
    )
    wallet_id = get_wallet_id(currency=currency, df_wallet=df_wallet)
    assert wallet_id == 3


def test_get_price():
    """Function to test get_price()"""
    currency = "AVAX"
    df_price = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "currency": ["BTC", "ETH", "DOGE", "ADA", "AVAX", "XRP"],
            "price": [25_000, 1_595, 0.064, 0.25, 12.48, 0.5],
        }
    )
    price = get_price(currency=currency, df_price=df_price)
    assert price == 12.48

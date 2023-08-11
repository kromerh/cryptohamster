from logger import (
    check_date,
    check_line,
    extract_date,
    extract_magnet,
)

import pytest
import pandas as pd


@pytest.mark.parametrize(
    "date, expected",
    [
        ("2021-08-a 13:59:59.763742", False),
        ("2021-08-8 13:59:59.763742", False),
        ("2021-8-08 13:59:59.763742", False),
        ("20-08-08 13:59:59.763742", False),
        ("2021-08-08 13:59.763742", False),
        ("2021-08-08 13:59:59.763742", True),
        ("2021-08-08 13:59:59.4444", True),
        ("2021-08-08 13:59:59", False),
        ("2021-08-08 23:59:59.763742", True),
    ],
)
def test_check_date(date, expected):
    """Function to test check_date() function."""
    assert check_date(date) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        ("2021-08-a 13:59:59.763742 - 0", False),
        ("2021-08-8 13:59:59.763742 - 0", False),
        ("2021-8-08 13:59:59.763742 - 0", False),
        ("20-08-08 13:59:59.763742 - 0", False),
        ("2021-08-08 13:59.763742 - 0", False),
        ("2021-08-08 13:59:59.763742 - 0", True),
        ("2021-08-08 13:59:59.4444 - 0", True),
        ("2021-08-08 13:59:59 - 0", False),
        ("2021-08-08 23:59:59.763742 - 0", True),
        ("2021-08-08 23:59:59.763742 - 1", True),
        ("2021-08-08 23:59:59.763742 - 2", False),
        ("2021-08-08 23:59:59.763742 - 3", False),
        ("2021-08-08 23:59:59.763742 - ", False),
        ("2021-08-08 23:59:59.763742", False),
    ],
)
def test_check_line(line, expected):
    """Function to test check_line() function."""
    assert check_line(line) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        ("2021-08-a 13:59:59.763742 - 0", "2021-08-a 13:59:59.763742"),
        ("2021-08-8 13:59:59.763742 - 0", "2021-08-8 13:59:59.763742"),
        ("2021-8-08 13:59:59.763742 - 0", "2021-8-08 13:59:59.763742"),
        ("20-08-08 13:59:59.763742 - 0", "20-08-08 13:59:59.763742"),
        ("2021-08-08 13:59.763742 - 0", "2021-08-08 13:59.763742"),
    ],
)
def test_extract_date(line, expected):
    """Function to test extract_date() function."""
    assert extract_date(line) == expected


@pytest.mark.parametrize(
    "line, expected",
    [
        ("2021-08-a 13:59:59.763742 - 0", "0"),
        ("2021-08-8 13:59:59.763742 - 0", "0"),
        ("2021-8-08 13:59:59.763742 - 0", "0"),
        ("20-08-08 13:59:59.763742 - 0", "0"),
        ("2021-08-08 13:59.763742 - 0", "0"),
        ("2021-08-08 13:59:59.763742 - 1", "1"),
        ("2021-08-08 13:59:59.4444 - 0", "0"),
        ("2021-08-08 13:59:59 - 0", "0"),
        ("2021-08-08 23:59:59.763742 - 3", "3"),
    ],
)
def test_extract_magnet(line, expected):
    """Function to test extract_magnet() function."""
    assert extract_magnet(line) == expected

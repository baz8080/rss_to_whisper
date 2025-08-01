import os
from unittest.mock import patch

import pytest

from src import utils


def test_is_writable_true():
    with patch("src.utils.os.access", return_value=True) as mock_access:
        result = utils.is_writable("/some/path")
        assert result is True
        mock_access.assert_called_once_with("/some/path", os.W_OK)


def test_is_writable_false():
    with patch("src.utils.os.access", return_value=False) as mock_access:
        result = utils.is_writable("/some/path")
        assert result is False
        mock_access.assert_called_once_with("/some/path", os.W_OK)


@pytest.mark.parametrize(
    "input_string, expected",
    [
        (None, ""),  # if None is allowed
        ("", ""),
        (" ", ""),
        ("file$name", "file-name"),
        ("hello__world", "hello-world"),
        ("trailing-", "trailing"),
        ("unsafe@chars!", "unsafe-chars"),
    ],
)
def test_escape_filename(input_string, expected):
    _escaped = utils.escape_filename(input_string)
    assert _escaped == expected


@pytest.mark.parametrize(
    "hours_minutes_seconds_sting, expected_seconds",
    [
        (None, 0),
        (" ", 0),
        ("unexpected string", 0),
        ("50", 50),
        ("1:30", 90),
        ("01:0", 60),
        ("1:1:15", 3675),
        ("01:02:20", 3740),
        ("::", 0),
        ("1:1:1:1", 219661),
    ],
)
def test_time_to_seconds(hours_minutes_seconds_sting, expected_seconds):
    _actual_seconds = utils.time_to_seconds(hours_minutes_seconds_sting)
    assert expected_seconds == _actual_seconds


@pytest.mark.parametrize(
    "parent_path, directory_name",
    [
        (None, "pods"),
        (None, ""),
        ("pods_parent", None),
        ("", None),
        (None, None),
        ("", ""),
    ],
)
def test_create_path_inputs(parent_path, directory_name):
    assert utils.create_path(parent_path, directory_name) is None


@pytest.mark.parametrize(
    "data, size, expected",
    [
        ([1, 2, 3, 4], 2, [[1, 2], [3, 4]]),
        ([1, 2, 3, 4, 5], 2, [[1, 2], [3, 4], [5]]),
        ([1, 2], 10, [[1, 2]]),
        ([1, 2, 3], 3, [[1, 2, 3]]),
        ([1, 2, 3], 1, [[1], [2], [3]]),
        ([], 3, []),
    ],
)
def test_chunk_valid_inputs(data, size, expected):
    assert list(utils.chunk(data, size)) == expected


def test_chunk_invalid_size_zero():
    data = [1, 2, 3]
    with pytest.raises(ValueError):
        list(utils.chunk(data, 0))


def test_chunk_invalid_size_negative():
    data = [1, 2, 3]
    with pytest.raises(ValueError):
        list(utils.chunk(data, -1))

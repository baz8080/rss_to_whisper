import os
import uuid
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock

import pytest

from src import utils


@pytest.mark.parametrize(
    "input_str, expected",
    [
        (str(uuid.uuid4()), True),  # valid UUID v4
        ("123e4567-e89b-12d3-a456-426614174000", True),  # valid UUID v1-style
        ("not-a-uuid", False),  # completely invalid
        ("12345678-1234-1234-1234-1234567890", False),  # too short
        ("", False),  # empty string
        ("   ", False),  # whitespace only
        (None, False),  # optional: None input
    ],
)
def test_is_valid_uuid(input_str, expected):
    result = utils.is_valid_uuid(input_str)
    assert result is expected


def test_is_valid_uuid_mock():
    with patch("src.utils.uuid", return_value=True) as mock_uuid_provider:
        is_valid_uuid = utils.is_valid_uuid("123e4567-e89b-12d3-a456-426614174000")
        assert is_valid_uuid is True
        mock_uuid_provider.UUID.assert_called_once_with(
            "123e4567-e89b-12d3-a456-426614174000"
        )


def test_get_hash_none():
    with pytest.raises(ValueError, match="Cannot hash None or empty string"):
        utils.get_hash(None)


def test_get_hash_interacts_with_md5():
    mock_md5 = Mock()
    mock_md5.hexdigest.return_value = "fakehash"

    with patch("src.utils.hashlib.md5", return_value=mock_md5):
        result = utils.get_hash("hello")

    mock_md5.update.assert_called_once_with(b"hello")
    mock_md5.hexdigest.assert_called_once()
    assert result == "fakehash"


def test_get_hash_known_value():
    hash_result = utils.get_hash("hello")
    assert hash_result == "5d41402abc4b2a76b9719d911017c592"


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


@patch("src.utils.Path")
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
def test_create_path_inputs(_path_mock, parent_path, directory_name):
    assert utils.create_path(parent_path, directory_name) is None


@patch("src.utils.escape_filename", side_effect=lambda x: x)
@patch("src.utils.Path")
def test_create_path_creates_directory(mock_path_cls, mock_escape):
    mock_path = MagicMock(spec=Path)
    mock_path.__truediv__.return_value = mock_path
    mock_path.exists.return_value = False
    mock_path_cls.return_value = mock_path

    result = utils.create_path("parent", "child")

    mock_path_cls.assert_called_once_with("parent")
    mock_escape.assert_called_once_with("child")
    mock_path.__truediv__.assert_called_once_with("child")
    mock_path.exists.assert_called_once()
    mock_path.mkdir.assert_called_once_with(parents=True)
    assert result == mock_path


@patch("src.utils.escape_filename", side_effect=lambda x: x)
@patch("src.utils.Path.exists", return_value=True)
@patch("src.utils.Path")
def test_create_path_does_not_create_if_exists(
    mock_path_cls, _mock_exists, _mock_escape
):
    mock_path = MagicMock(spec=Path)
    mock_path.__truediv__.return_value = mock_path
    mock_path.exists.return_value = True
    mock_path_cls.return_value = mock_path

    result = utils.create_path("parent", "child")

    mock_path.mkdir.assert_not_called()
    assert result == mock_path


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

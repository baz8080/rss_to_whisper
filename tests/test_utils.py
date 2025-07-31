import os

import pytest
import utils
from unittest.mock import patch

@pytest.mark.parametrize("input_string, expected", [
    (None, ""),        # if None is allowed
    ("", ""),
    (" ", ""),
    ("file$name", "file-name"),
    ("hello__world", "hello-world"),
    ("trailing-", "trailing"),
    ("unsafe@chars!", "unsafe-chars"),
])
def test_escape_filename(input_string, expected):
    _escaped = utils.escape_filename(input_string)
    assert _escaped == expected

def test_is_writable_true():
    with patch("utils.os.access", return_value=True) as mock_access:
        result = utils.is_writable("/some/path")
        assert result is True
        mock_access.assert_called_once_with("/some/path", os.W_OK)

def test_is_writable_false():
    with patch("utils.os.access", return_value=False) as mock_access:
        result = utils.is_writable("/some/path")
        assert result is False
        mock_access.assert_called_once_with("/some/path", os.W_OK)
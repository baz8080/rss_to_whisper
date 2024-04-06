import hashlib
import os
import re
import uuid


def is_valid_uuid(uuid_str: str):
    try:
        uuid.UUID(uuid_str)
        return True
    except ValueError:
        return False


def get_partial_guid(_uuid_str: str, _content: str):
    if _uuid_str and is_valid_uuid(_uuid_str):
        return _uuid_str[0:8]
    elif _content:
        ha = hashlib.md5()
        ha.update(_content.encode("utf-8"))
        digest = ha.hexdigest()
        return str(digest)[0:8]
    else:
        return "noguid"


def is_writable(path):
    return os.access(path, os.W_OK)


def escape_filename(_filename: str):
    _escaped = "".join([x if x.isalnum() else "-" for x in _filename])
    _escaped = re.sub(r'-{2,}', '-', _escaped)
    _escaped = _escaped[:-1] if _escaped.endswith('-') else _escaped

    return _escaped


def time_to_seconds(time_str: str):
    # https://stackoverflow.com/a/6402934
    return sum(float(x) * 60 ** i for i, x in enumerate(reversed(time_str.split(':'))))

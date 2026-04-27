""" "
Flattener
Converts arbitrarily nested JSON objects into flat dictionaries suitable for relational databases and CSV files.

Example:
Input:
{"trainer": {"name": "Ash"}, "badges": [{"id": 1}, {"id": 2}]}

Output:
{"trainer_name": "Ash", "badges_0_id": 1, "badges_1_id": 2}
"""

from typing import Any


def flatten(obj: Any, prefix: str = "", sep: str = "_") -> dict[str, Any]:
    """
    Recursively flatten a nested dict/list into a flat dict.
    Keys are joined with `sep`.
    """
    result: dict[str, Any] = {}

    if isinstance(obj, dict):
        for key, value in obj.items():
            new_key = f"{prefix}{sep}{key}" if prefix else key
            result.update(flatten(value, prefix=new_key, sep=sep))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            new_key = f"{prefix}{sep}{i}" if prefix else str(i)
            result.update(flatten(item, prefix=new_key, sep=sep))

    else:
        # Scalar value — base case
        result[prefix] = obj

    return result


def flatten_records(records: list[dict]) -> list[dict]:
    """
    Flatten a list of JSON records.
    """
    return [flatten(r) for r in records]


def apply_transform(record: dict, transform) -> dict:
    """
    Apply rename, include, exclude transforms to a flat record.
    `transform` is a TransformConfig instance.
    """
    # Rename fields
    for old, new in transform.rename.items():
        if old in record:
            record[new] = record.pop(old)

    # Exclude fields
    for field in transform.exclude:
        record.pop(field, None)

    # Include filter — only keep specified fields
    if transform.include:
        record = {k: v for k, v in record.items() if k in transform.include}

    return record


def extract_results(response: dict | list, results_path: str) -> list[dict]:
    """
    Navigate into a response using dot-notation path to find the list of records.
    e.g. results_path="data.items" on {"data": {"items": [...]}}
    If the response itself is a list, return it directly.
    If results_path is empty or ".", return [response] (single record).
    """
    if isinstance(response, list):
        return response

    if not results_path or results_path == ".":
        return [response]

    parts = results_path.split(".")
    current = response
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            # Path not found — treat whole response as single record
            return [response]

    if isinstance(current, list):
        return current
    else:
        return [current]

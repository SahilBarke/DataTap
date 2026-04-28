"""
Flattener
Converts arbitrarily nested JSON objects into flat dictionaries suitable for relational databases and CSV files.

Example:
Input:
{"trainer": {"name": "Ash"}, "badges": [{"id": 1}, {"id": 2}]}

Output:
{"trainer_name": "Ash", "badges_0_id": 1, "badges_1_id": 2}
"""

from typing import Any
from dataclasses import dataclass, field


# Transform configuration
@dataclass
class TransformConfig:
    rename: dict[str, str] = field(default_factory=dict)
    exclude: list[str] = field(default_factory=list)
    include: list[str] = field(default_factory=list)


# Core flattener
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
        # scalar value (base case)
        result[prefix] = obj

    return result


def flatten_records(records: list[dict]) -> list[dict]:
    """
    Flatten a list of JSON records.
    """
    return [flatten(r) for r in records]


# Transform layer
def apply_transform(
    record: dict[str, Any], transform: TransformConfig
) -> dict[str, Any]:
    """
    Apply rename, exclude, include transformations safely.
    Does NOT mutate the original record.
    """
    record = dict(record)  # avoid mutation side effects

    # Rename fields
    for old, new in transform.rename.items():
        if old in record:
            record[new] = record.pop(old)

    # Exclude fields
    for field in transform.exclude:
        record.pop(field, None)

    # Include filter (whitelist mode)
    if transform.include:
        record = {k: v for k, v in record.items() if k in transform.include}

    return record


# Extraction helper
def extract_results(response: dict | list, results_path: str) -> list[dict]:
    """
    Navigate into a response using dot-notation path to find records.

    Example:
        results_path="data.items"
        {"data": {"items": [...]}}

    If response is a list → return it directly.
    If path not found → return [response].
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
            return [response]  # fallback

    if isinstance(current, list):
        return current

    return [current]

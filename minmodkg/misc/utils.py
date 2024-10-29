from __future__ import annotations

from typing import Any, Optional, TypeVar

V = TypeVar("V")


def batch(size: int, *vars, return_tuple: bool = False):
    """Batch the variables into batches of size. When vars is a single variable,
    it will return a list of batched values instead of list of tuple of batched values.

    If we want to batch a single variable to a list of tuple of batched values, set
    return_tuple to True.
    """
    output = []
    n = len(vars[0])
    if len(vars) == 1 and not return_tuple:
        for i in range(0, n, size):
            output.append(vars[0][i : i + size])
    else:
        for i in range(0, n, size):
            output.append(tuple(var[i : i + size] for var in vars))
    return output


def group_by_key(
    output: list[dict], key: str, value: Optional[str] = None
) -> dict[str, list]:
    groups = {}
    if value is None:
        for row in output:
            val = row[key]
            if val not in groups:
                groups[val] = []
            groups[val].append(row)
    else:
        for row in output:
            val = row[key]
            if val not in groups:
                groups[val] = []
            groups[val].append(row[value])
    return groups


def group_by_attr(output: list[V], attr: str) -> dict[str, list[V]]:
    groups = {}
    for row in output:
        val = getattr(row, attr)
        if val not in groups:
            groups[val] = []
        groups[val].append(row)
    return groups


def assert_isinstance(x: Any, cls: type[V]) -> V:
    assert isinstance(x, cls), x
    return x

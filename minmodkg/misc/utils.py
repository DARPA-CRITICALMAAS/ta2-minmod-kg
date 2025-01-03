from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence, TypeVar

from rdflib import XSD, Graph, Literal
from rdflib.term import Node

V = TypeVar("V")


def norm_literal(val: Node) -> Node:
    """Normalize the literal value between triple store and python. For example, xsd.decimal saved to Virtuoso is converted into xsd.double when read back."""
    if isinstance(val, Literal):
        if val.datatype in {XSD.decimal, XSD.float, XSD.double}:
            return Literal(val.value, datatype=XSD.double)
        elif val.datatype is None or val.datatype == XSD.string:
            return Literal(str(val.value), datatype=None, lang=val.language)
    return val


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


def assert_not_none(x: Optional[V]) -> V:
    assert x is not None
    return x


def assert_isinstance(x: Any, cls: type[V]) -> V:
    assert isinstance(x, cls), x
    return x


def file_ident(file: str | Path):
    file = Path(file).resolve()
    filehash = sha256(file.read_bytes()).hexdigest()
    return f"{file}::{filehash}"


def mut_merge_graphs(graphs: Sequence[Graph]) -> Graph:
    if len(graphs) == 0:
        raise ValueError("No graphs to merge")

    graph = graphs[0]
    for g in graphs[1:]:
        graph += g
    return graph


def filter_duplication(
    lst: Iterable[V], key_fn: Optional[Callable[[V], Any]] = None
) -> list[V]:
    keys = set()
    new_lst = []
    if key_fn is not None:
        for item in lst:
            k = key_fn(item)
            if k in keys:
                continue

            keys.add(k)
            new_lst.append(item)
    else:
        for k in lst:
            if k in keys:
                continue
            keys.add(k)
            new_lst.append(k)
    return new_lst


def extend_unique(unique_lst: list[V], other_lst: Iterable[V]) -> list[V]:
    keys = set(unique_lst)
    for item in other_lst:
        if item not in keys:
            unique_lst.append(item)
            keys.add(item)
    return unique_lst


def exclude_none_or_empty_list(obj: dict):
    return {
        k: v
        for k, v in obj.items()
        if v is not None and (not isinstance(v, Sequence) or len(v) > 0)
    }

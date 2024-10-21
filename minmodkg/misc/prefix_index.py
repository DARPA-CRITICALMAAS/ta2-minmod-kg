from __future__ import annotations

from collections import defaultdict
from typing import Generic, Optional, Union

from minmodkg.misc.utils import V


class LongestPrefixIndex(Generic[V]):
    """Given a list of prefixes, we want to find the longest predefined prefixes of a given string."""

    def __init__(
        self,
        index: dict[str, LongestPrefixIndex | str],
        start: int,
        end: int,
    ) -> None:
        self.index = index
        self.start = start
        self.end = end

    @staticmethod
    def create(prefixes: list[str]):
        sorted_prefixes = sorted(prefixes, key=lambda x: len(x), reverse=True)
        if len(sorted_prefixes) == 0:
            raise Exception("No prefix provided")
        return LongestPrefixIndex._create(sorted_prefixes, 0)

    @staticmethod
    def _create(sorted_prefixes: list[str], start: int):
        shortest_ns = sorted_prefixes[-1]
        index = LongestPrefixIndex({}, start, len(shortest_ns))

        if index.start == index.end:
            index.index[""] = shortest_ns
            subindex = LongestPrefixIndex._create(sorted_prefixes[:-1], index.end)
            for key, node in subindex.index.items():
                assert key not in index.index
                index.index[key] = node
            index.end = subindex.end
            return index

        tmp = defaultdict(list)
        for i, prefix in enumerate(sorted_prefixes):
            key = prefix[index.start : index.end]
            tmp[key].append(i)

        for key, lst_prefix_idx in tmp.items():
            if len(lst_prefix_idx) == 1:
                index.index[key] = sorted_prefixes[lst_prefix_idx[0]]
            else:
                index.index[key] = LongestPrefixIndex._create(
                    [sorted_prefixes[i] for i in lst_prefix_idx], index.end
                )
        return index

    def get(self, s: str) -> Optional[str]:
        """Get prefix of a string. Return None if it is not found"""
        key = s[self.start : self.end]
        if key in self.index:
            prefix = self.index[key]
            if isinstance(prefix, LongestPrefixIndex):
                return prefix.get(s)
            return prefix if s.startswith(prefix) else None

        if "" in self.index:
            prefix = self.index[""]
            assert isinstance(prefix, str)
            return prefix if s.startswith(prefix) else None

        return None

    def __str__(self):
        """Readable version of the index"""
        stack: list[tuple[int, str, Union[str, LongestPrefixIndex]]] = list(
            reversed([(0, k, v) for k, v in self.index.items()])
        )
        out = []

        while len(stack) > 0:
            depth, key, value = stack.pop()
            indent = "    " * depth
            if isinstance(value, str):
                out.append(indent + "`" + key + "`: " + value + "\n")
            else:
                out.append(indent + "`" + key + "`:" + "\n")
                for k, v in value.index.items():
                    stack.append((depth + 1, k, v))

        return "".join(out)

    def to_dict(self):
        return {
            k: v.to_dict() if isinstance(v, LongestPrefixIndex) else v
            for k, v in self.index.items()
        }

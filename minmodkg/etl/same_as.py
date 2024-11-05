from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import networkx as nx
import orjson
import serde.csv
import serde.json
from joblib import Parallel, delayed
from libactor.cache import cache
from tqdm import tqdm

from statickg.helper import FileSqliteBackend, Fn, logger_helper
from statickg.models.etl import ETLOutput
from statickg.models.file_and_path import (
    FormatOutputPath,
    FormatOutputPathModel,
    InputFile,
    RelPath,
)
from statickg.models.repository import Repository
from statickg.services.interface import BaseFileService, BaseService

"""
Create a dedup group sites that are the same as each other.

This service works in two steps:

1. For each file, it produces a mapping from each site to a local dedup group.
2. Then, we map each local dedup to a final dedup site. Hopefully this graph is much smaller and much sparse.
3. Then, we generate same-as and dedup group.

"""


class SameAsServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class SameAsServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    output: RelPath | FormatOutputPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class SameAsService(BaseFileService[SameAsServiceInvokeArgs]):
    def __init__(
        self,
        name: str,
        workdir: Path,
        args: SameAsServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)
        self.parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

    def forward(
        self, repo: Repository, args: SameAsServiceInvokeArgs, tracker: ETLOutput
    ):
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )
        output_fmter = FormatOutputPathModel.init(args["output"])

        jobs = []
        for infile in infiles:
            outfile = output_fmter.get_outfile(infile.path)
            outfile.parent.mkdir(parents=True, exist_ok=True)
            group_prefix = outfile.parent / outfile.stem
            jobs.append((group_prefix, infile, outfile))

        readable_ptns = self.get_readable_patterns(args["input"])

        if self.parallel:
            it: Iterable = self.parallel_executor(
                delayed(step1_exec)(self.workdir, group_prefix, infile, outfile)
                for group_prefix, infile, outfile in jobs
            )
        else:
            it: Iterable = (
                step1_exec(self.workdir, group_prefix, infile, outfile)
                for group_prefix, infile, outfile in jobs
            )

        outfiles = set()
        for outfile in tqdm(
            it, desc=f"Generating same-as for {readable_ptns}", disable=self.verbose < 1
        ):
            outfiles.add(outfile.relative_to(output_fmter.outdir))

        self.remove_unknown_files(outfiles, output_fmter.outdir)

        # after getting step 1, we need to read all of the files and generate the final dedup group

    def step2(self, infiles: list[Path]):
        # get all site that are the same as each other
        mapping = defaultdict(list)
        for file in infiles:
            for site, group in orjson.loads(file.read_bytes()).items():
                mapping[site].append(group)

        G = nx.from_edgelist(
            [
                (groups[0], groups[i])
                for site, groups in mapping.items()
                if len(groups) > 1
                for i in range(1, len(groups))
            ]
        )
        final_dedup_groups = nx.connected_components(G)


@dataclass
class GraphLink:
    node2group: dict[str, str]
    groups: dict[str, list[str]]

    def replace_group(self, new_groups: list[list[str]]):
        """Update the linking by deleting existing groups and add them back"""
        affected_groups = {}
        affected_nodes  = set()
        for grp in new_groups:
            for u in grp:
                affected_nodes.add(u)
                grp_id = self.node2group[u]
                affected_groups[grp_id] = self.groups[grp_id]
                
        # the affected groups need to be updated -- first step is to remove all of them.
        for grp_id in affected_groups.keys():
            del self.groups[grp_id]

        for grp in affected_groups.values():
            new_nodes = [u for u in grp if u not in affected_nodes]
            if len(new_nodes) > 0:
                new_groups.append(new_nodes)

        # then we add new groups
        for grp in new_groups:
            grp_id = self.get_group_id(grp)
            for u in grp:
                self.node2group[u] = grp_id
            self.groups[grp_id] = grp

    
    def get_group_id(self, nodes: list[str]) -> str:
        raise NotImplementedError()
        

    def remove_link(self, u: str, v: str, edits: dict[str, set[str]]):
        """Remove a link from source u to target t from the graph"""
        u_grp = self.node2group[u]
        v_grp = self.node2group[v]

        if u_grp != v_grp:
            # nothing to do
            return

        nodes = self.groups[u_grp]
        if len(nodes) == 2:
            # we need to create two separate groups.
            ...
        else:
            # if things work correctly, we must have another node t same as u or v, so we can decide that we remove u or v
            # from the existing group.
            if edits[u].isdisjoint(nodes):
                if edits[v].isdisjoint(nodes)


def apply_group_edits(
    groups: dict[str, list[str]], edits: dict[str, list[tuple[str, str]]]
): ...


def step1_exec(workdir: Path, prefix: str, infile: InputFile, outfile: Path):
    return Step1Fn.get_instance(workdir).same_as_step1_exec(prefix, infile, outfile)


class Step1Fn(Fn):
    @cache(
        backend=FileSqliteBackend.factory(filename="same_as_step1_exec.v100.sqlite"),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def same_as_step1_exec(self, prefix: str, infile: InputFile, outfile: Path):
        edges = serde.csv.deser(infile.path)
        G = nx.from_edgelist(edges[1:])
        groups = nx.connected_components(G)

        mapping = {
            f"{prefix}:{gid}": list(group) for gid, group in enumerate(groups, start=1)
        }
        serde.json.ser(mapping, outfile)
        return outfile

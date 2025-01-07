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
from minmodkg.models.base import MINMOD_KG
from rdflib import OWL
from slugify import slugify
from timer import Timer
from tqdm import tqdm

from statickg.helper import FileSqliteBackend, Fn
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
    curated_input: RelPath | list[RelPath]
    output: RelPath | FormatOutputPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class SameAsService(BaseFileService[SameAsServiceConstructArgs]):
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
        timer = Timer()

        with timer.watch("[same-as] resolve dedup group"):
            subgroup_files = self.step1_compute_subgroup(repo, args)
            graphlink = self.step2_resolve_final_group(subgroup_files, args)

        with timer.watch("[same-as] update dedup group"):
            graphlink = self.step3_update_group(graphlink, args)

        with timer.watch("[same-as] save dedup group"):
            # finally, we save the group and generate same-as link.
            self.step4_save(graphlink, args)

        timer.report(self.logger.info)

    def step1_compute_subgroup(self, repo: Repository, args: SameAsServiceInvokeArgs):
        """Compute subgroup for each same as file"""
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "step_1"

        jobs = []
        for infile in infiles:
            outfile = output_fmter.get_outfile(infile.path)
            outfile.parent.mkdir(parents=True, exist_ok=True)
            group_prefix = outfile.parent / outfile.stem
            jobs.append(
                (
                    slugify(str(group_prefix.relative_to(output_fmter.outdir))),
                    infile,
                    outfile,
                )
            )

        readable_ptns = self.get_readable_patterns(args["input"])

        if self.parallel:
            it: Iterable = self.parallel_executor(
                delayed(Step1ComputingSubGroupFn.exec)(
                    self.workdir, prefix=group_prefix, infile=infile, outfile=outfile
                )
                for group_prefix, infile, outfile in jobs
            )
        else:
            it: Iterable = (
                Step1ComputingSubGroupFn.exec(
                    self.workdir, prefix=group_prefix, infile=infile, outfile=outfile
                )
                for group_prefix, infile, outfile in jobs
            )

        outfiles = set()
        for outfile in tqdm(
            it,
            total=len(jobs),
            desc=f"Generating same-as for {readable_ptns}",
            disable=self.verbose < 1,
        ):
            outfiles.add(outfile.relative_to(output_fmter.outdir))

        self.remove_unknown_files(outfiles, output_fmter.outdir)
        return [output_fmter.outdir / outfile for outfile in outfiles]

    def step2_resolve_final_group(
        self, subgroup_files: list[Path], args: SameAsServiceInvokeArgs
    ):
        site2subgroups = defaultdict(list)
        id2subgroups = defaultdict(list)
        for file in sorted(subgroup_files):
            subgrp = orjson.loads(file.read_bytes())
            assert all(k not in id2subgroups for k in subgrp.keys())
            id2subgroups.update(subgrp)
            for grpid, grp in subgrp.items():
                for site_id in grp:
                    site2subgroups[site_id].append(grpid)

        # create a mapping from subgroup -> final group
        G = nx.from_edgelist(
            [
                (subgrps[0], subgrps[i])
                for site, subgrps in site2subgroups.items()
                if len(subgrps) > 1
                for i in range(1, len(subgrps))
            ]
        )
        final_grps: list[list[str]] = nx.connected_components(G)
        final_grps = sorted((sorted(grp) for grp in final_grps), key=lambda grp: grp[0])

        sub2final_grp = {}
        for i, grps in enumerate(final_grps, start=1):
            final_grp_id = f"grp2__{i}"
            for sub_grp_id in grps:
                sub2final_grp[sub_grp_id] = final_grp_id
        for sub_grp_id in id2subgroups.keys():
            if sub_grp_id not in sub2final_grp:
                sub2final_grp[sub_grp_id] = sub_grp_id

        site2groups = {}
        id2setgroups = defaultdict(set)

        for sub_grp_id, final_grp_id in sub2final_grp.items():
            for site in id2subgroups[sub_grp_id]:
                id2setgroups[final_grp_id].add(site)
                if site in site2groups:
                    assert site2groups[site] == final_grp_id, site2groups[site]
                else:
                    site2groups[site] = final_grp_id

        id2groups = {
            final_grp_id: sorted(grps) for final_grp_id, grps in id2setgroups.items()
        }
        return GraphLink(site2groups, id2groups)

    def step3_update_group(self, graph_link: GraphLink, args: SameAsServiceInvokeArgs):
        return graph_link

    def step4_save(self, graph_link: GraphLink, args: SameAsServiceInvokeArgs):
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "final"
        output_fmter.outdir.mkdir(parents=True, exist_ok=True)

        serde.json.ser(
            list(graph_link.groups.values()), output_fmter.outdir / "groups.json"
        )

        with open(output_fmter.outdir / "same_as.ttl", "w") as f:
            mr = MINMOD_KG.ns.mr
            f.write(f"@prefix : <{mr.namespace}> .\n")
            f.write(f"@prefix owl: <{str(OWL)}> .\n\n")

            for group, nodes in graph_link.groups.items():
                if len(nodes) == 1:
                    f.write(f":{nodes[0]} owl:sameAs :{nodes[0]} .\n")
                else:
                    for i in range(1, len(nodes)):
                        f.write(f":{nodes[0]} owl:sameAs :{nodes[i]} .\n")


@dataclass
class GraphLink:
    node2group: dict[str, str]
    groups: dict[str, list[str]]

    @staticmethod
    def from_connected_components(components: list[list[str]]):
        node2group = {}
        groups: dict[str, list[str]] = {}
        for grp in components:
            grp_id = GraphLink.get_group_id(grp)
            groups[grp_id] = grp
            node2group.update((u, grp_id) for u in grp)
        return GraphLink(node2group, groups)

    def replace_group(self, new_groups: list[list[str]]):
        """Update the linking by deleting existing groups and add them back"""
        affected_groups = {}
        affected_nodes = set()
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

    @staticmethod
    def get_group_id(nodes: list[str]) -> str:
        return "grp_" + min(nodes)


class Step1ComputingSubGroupFn(Fn):

    @cache(
        backend=FileSqliteBackend.factory(filename="step_1_v105.sqlite"),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, prefix: str, infile: InputFile, outfile: Path):
        mr = MINMOD_KG.ns.mr
        lst = serde.csv.deser(infile.path)
        it = iter(lst)
        next(it)
        edges = []
        for uid, vid in it:
            assert not uid.startswith("http")
            assert not vid.startswith("http")
            edges.append((uid, vid))

        G = nx.from_edgelist(edges)
        groups = nx.connected_components(G)

        mapping = {
            f"grp1__{prefix}__{gid}": list(group)
            for gid, group in enumerate(groups, start=1)
        }
        serde.json.ser(mapping, outfile)
        return outfile

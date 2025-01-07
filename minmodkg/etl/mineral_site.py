from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import orjson
import serde.json
import xxhash
from joblib import delayed
from libactor.cache import cache
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.mineral_site import MineralSite as RelMineralSite
from minmodkg.services.mineral_site_v2 import MineralSiteService
from minmodkg.transformations import make_site_uri
from minmodkg.typing import InternalID
from rdflib import RDFS, Graph
from slugify import slugify
from timer import Timer
from tqdm import tqdm

from statickg.helper import FileSqliteBackend, get_parallel_executor
from statickg.models.etl import ETLOutput
from statickg.models.file_and_path import InputFile, RelPath
from statickg.models.repository import Repository
from statickg.services.interface import BaseFileService, BaseService


class MineralSiteETLServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class MineralSiteETLServiceInvokeArgs(TypedDict):
    predefined_entity_dir: RelPath
    input: RelPath | list[RelPath]
    output: RelPath
    same_as_group: RelPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class MineralSiteETLService(BaseFileService[MineralSiteETLServiceConstructArgs]):
    """Merge the mineral site data across multiple files"""

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: MineralSiteETLServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)

    def forward(
        self, repo: Repository, args: MineralSiteETLServiceInvokeArgs, output: ETLOutput
    ):
        """Execute the merge process"""
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )

        timer = Timer()
        with timer.watch_and_report("Partitioning the data"):
            partition_output = self.partition(args, infiles)
        with timer.watch_and_report("Resolving the same as"):
            same_as_output = self.resolve_same_as(args, partition_output)
        with timer.watch_and_report("Merging the data"):
            self.merge(args, partition_output, same_as_output)

    def partition(
        self, args: MineralSiteETLServiceInvokeArgs, infiles: list[InputFile]
    ) -> dict[Path, list[InputFile]]:
        outdir = args["output"].get_path()
        partition_outdir = outdir / "parts"
        partition_outdir.mkdir(parents=True, exist_ok=True)

        # partition the data into source directories
        jobs = infiles
        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(PartitionFn.exec)(
                self.workdir,
                infile=infile,
                outdir=partition_outdir,
            )
            for infile in jobs
        )
        part_outfiles: set[Path] = set()
        for lst in tqdm(
            it, total=len(jobs), desc="Partitioning the data", disable=self.verbose < 1
        ):
            part_outfiles.update(lst)
        self.remove_unknown_files(part_outfiles, partition_outdir)

        output: dict[Path, list[InputFile]] = defaultdict(list)
        for outfile in part_outfiles:
            relpath = outfile.relative_to(partition_outdir)
            out_relpath = Path(relpath.parts[0]) / relpath.parts[1]
            output[out_relpath].append(
                InputFile.from_relpath(
                    RelPath(
                        basetype=args["output"].basetype,
                        basepath=args["output"].basepath,
                        relpath=str(outfile.relative_to(args["output"].basepath)),
                    )
                )
            )
        return output

    def resolve_same_as(
        self,
        args: MineralSiteETLServiceInvokeArgs,
        partition_files: dict[Path, list[InputFile]],
    ) -> dict[Path, InputFile]:
        same_as_outdir = args["output"].get_path() / "same_as"
        same_as_outdir.mkdir(parents=True, exist_ok=True)

        file2partition = {}
        for group, files in partition_files.items():
            for file in files:
                file2partition[file.path] = group

        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(read_site_ids)(infile.path)
            for infiles in partition_files.values()
            for infile in infiles
        )
        sites: dict[InternalID, dict] = {}
        partitions: dict[Path, set[InternalID]] = defaultdict(set)
        for file, d in tqdm(it):
            sites.update(d)
            partitions[file2partition[file]].update(d.keys())

        groups: list[list[InternalID]] = [
            filtered_lst
            for lst in serde.json.deser(args["same_as_group"].get_path())
            if len((filtered_lst := [id for id in lst if id in sites])) > 0
        ]
        linked_sites = {site for grp in groups for site in grp}
        # add sites that are not linked first
        for site_id, site in sites.items():
            if site_id not in linked_sites:
                groups.append([site_id])

        # assert no overlapping between groups
        seen_ids = set()
        for grp in groups:
            for site_id in grp:
                assert site_id not in seen_ids
                seen_ids.add(site_id)

        for grp in groups:
            dedup_id = RelMineralSite.get_dedup_id(grp)
            for site_id in grp:
                sites[site_id]["dedup_id"] = dedup_id

        # save the results
        output: dict[Path, InputFile] = {}
        output_files: set[Path] = set()
        for out_relpath, site_ids in partitions.items():
            outfile_relpath = out_relpath.parent / (out_relpath.name + "__same_as.json")
            outfile = same_as_outdir / outfile_relpath
            outfile.parent.mkdir(parents=True, exist_ok=True)
            serde.json.ser(
                [
                    {
                        "site_id": site_id,
                        "dedup_id": sites[site_id]["dedup_id"],
                    }
                    for site_id in sorted(site_ids)
                ],
                outfile,
            )
            output_files.add(outfile_relpath)
            output[out_relpath] = InputFile.from_relpath(
                RelPath(
                    args["output"].basetype,
                    args["output"].basepath,
                    str(outfile.relative_to(args["output"].basepath)),
                )
            )

        self.remove_unknown_files(output_files, same_as_outdir)
        return output

    def merge(
        self,
        args: MineralSiteETLServiceInvokeArgs,
        partition_files: dict[Path, list[InputFile]],
        same_as_files: dict[Path, InputFile],
    ):
        outdir = args["output"].get_path()

        merged_outdir = outdir / "merged"
        merged_outdir.mkdir(parents=True, exist_ok=True)

        predefined_entity_dir = args["predefined_entity_dir"].get_path()
        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(MergeFn.exec)(
                self.workdir,
                predefined_entity_dir,
                infiles=infiles,
                sameas_file=same_as_files[out_relpath],
                outdir=merged_outdir / out_relpath,
            )
            for out_relpath, infiles in partition_files.items()
        )
        merged_outfiles = set()
        for file in tqdm(
            it,
            total=len(partition_files),
            desc="Merging the data",
            disable=self.verbose < 1,
        ):
            merged_outfiles.add(file)
        self.remove_unknown_files(merged_outfiles, merged_outdir)


def read_site_ids(infile: Path) -> tuple[Path, dict[InternalID, dict]]:
    """Read the site ids from the file"""
    lst = serde.json.deser(infile)
    output = {}
    for r in lst:
        site_id = make_site_uri(r["source_id"], r["record_id"], namespace="")
        output[site_id] = {
            "site_id": site_id,
            "source_id": r["source_id"],
            "record_id": r["record_id"],
        }
    return (infile, output)


class PartitionFn:
    """Partition the mineral sites from a single file into <source_id>/<bucket>/<file_name>.json"""

    instances = {}
    num_buckets = 256

    def __init__(self, workdir: Path):
        self.workdir = workdir

    @staticmethod
    def get_instance(workdir: Path) -> PartitionFn:
        if workdir not in PartitionFn.instances:
            PartitionFn.instances[workdir] = PartitionFn(workdir)
        return PartitionFn.instances[workdir]

    @classmethod
    def exec(cls, workdir: Path, **kwargs) -> list[Path]:
        return cls.get_instance(workdir).invoke(**kwargs)

    @cache(
        backend=FileSqliteBackend.factory(
            filename="partition-v100.sqlite", multi_files=True
        ),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outdir: Path) -> list[Path]:
        lst = serde.json.deser(infile.path)
        source_ids = list({r["source_id"] for r in lst})
        source_id_names = [
            slugify(source_id).replace("-", "_") for source_id in source_ids
        ]

        outfile_subpath = infile.relpath.replace("/", "__") + ".gz"

        outfiles: list[Path] = []
        source2records = {
            source_id: [[] for _ in range(self.num_buckets)] for source_id in source_ids
        }
        for r in lst:
            bucketno = (
                xxhash.xxh64(str(r["record_id"]).encode()).intdigest()
                % self.num_buckets
            )
            source2records[r["source_id"]][bucketno].append(r)

        for source_id, source_id_name in zip(source_ids, source_id_names):
            for bucketno, records in enumerate(source2records[source_id]):
                if len(records) == 0:
                    continue
                outfile = outdir / source_id_name / f"b{bucketno:03d}" / outfile_subpath
                outfile.parent.mkdir(parents=True, exist_ok=True)
                serde.json.ser(records, outfile)
                outfiles.append(outfile)

        return outfiles


class MergeFn:
    """Merge multiple files of the same source into a single file"""

    instances = {}

    def __init__(self, workdir: Path, predefined_entity_dir: Path):
        self.workdir = workdir

        g = Graph()
        g.parse(predefined_entity_dir / "material_form.ttl", format="ttl")
        self.material_form_conversion = {
            str(subj): float(obj.value)  # type: ignore
            for subj, obj in g.subject_objects(MINMOD_NS.mo.uri("conversion"))
        }

        g = Graph()
        g.parse(predefined_entity_dir / "epsg.ttl", format="ttl")
        self.epsg_name = {
            str(subj): str(obj.value)  # type: ignore
            for subj, obj in g.subject_objects(RDFS.label)
        }

    @staticmethod
    def get_instance(workdir: Path, predefined_entity_dir: Path) -> MergeFn:
        if workdir not in MergeFn.instances:
            MergeFn.instances[workdir] = MergeFn(workdir, predefined_entity_dir)
        return MergeFn.instances[workdir]

    @classmethod
    def exec(cls, workdir: Path, predefined_entity_dir: Path, **kwargs) -> Path:
        return cls.get_instance(workdir, predefined_entity_dir).invoke(**kwargs)

    @cache(
        backend=FileSqliteBackend.factory(filename="merge-v101.sqlite"),
        cache_ser_args={
            "infiles": lambda lst: orjson.dumps(
                sorted(x.get_ident() for x in lst)
            ).decode(),
            "sameas_file": lambda x: x.get_ident(),
        },
    )
    def invoke(
        self, infiles: list[InputFile], sameas_file: InputFile, outdir: Path
    ) -> Path:
        id2sites = defaultdict(list)
        for infile in sorted(infiles, key=lambda x: x.path):
            for r in serde.json.deser(infile.path):
                id2sites[r["record_id"]].append(r)

        dedup_map = {
            r["site_id"]: r["dedup_id"] for r in serde.json.deser(sameas_file.path)
        }

        outdir.mkdir(parents=True, exist_ok=True)
        outfile = outdir / "merged.json.gz"

        # merge the data
        output = []
        for raw_sites in id2sites.values():
            site = MineralSite.from_dict(raw_sites[0])
            for raw_site in raw_sites[1:]:
                site.merge_mut(MineralSite.from_dict(raw_site))

            norm_site = RelMineralSite.from_raw_site(
                site.to_dict(), self.material_form_conversion, self.epsg_name
            )
            norm_site.dedup_site_id = dedup_map[norm_site.site_id]
            output.append(norm_site.to_dict())

        serde.json.ser(output, outfile)
        return outfile

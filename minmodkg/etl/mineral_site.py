from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, Optional, TypedDict

import orjson
import serde.json
import xxhash
from joblib import delayed
from libactor.cache import cache
from minmodkg.misc.utils import group_by
from minmodkg.models.kg.base import MINMOD_KG, MINMOD_NS
from minmodkg.models.kg.data_source import DataSource
from minmodkg.models.kg.mineral_site import MineralSite
from minmodkg.models.kgrel.dedup_mineral_site import DedupMineralSite
from minmodkg.models.kgrel.mineral_site import MineralSite as RelMineralSite
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.services.kgrel_entity import FileEntityService
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

COMPRESSION: str = ".lz4"
# COMPRESSION: str = ""


class MineralSiteETLServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class MineralSiteETLServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    output: RelPath

    entity_dir: RelPath

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
        with timer.watch_and_report("Deduping the data"):
            dedup_output = self.dedup(args, partition_output)
        with timer.watch_and_report("Merging the data"):
            merge_output = self.merge(args, partition_output, dedup_output)
        with timer.watch_and_report("Preparing KG input"):
            self.prep_kg_input(args, merge_output)
        with timer.watch_and_report("Preparing KGRel input"):
            self.prep_kgrel_input(args, merge_output)

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

    def dedup(
        self,
        args: MineralSiteETLServiceInvokeArgs,
        partition_files: dict[Path, list[InputFile]],
    ) -> dict[Path, InputFile]:
        def read_site_data(
            infile: Path,
        ) -> tuple[Path, dict[InternalID, dict]]:
            """Read the site ids from the file"""
            lst = serde.json.deser(infile)
            output = {}
            for r in lst:
                site = MineralSite.from_dict(r)
                output[site.id] = {
                    "site_id": site.id,
                    "source_id": site.source_id,
                    "record_id": site.record_id,
                    "extra": {
                        # needed to compute the dedup information
                        # "source_score": r["source_score"],
                        # "created_by": r.created_by,
                        # "modified_at": datetime.fromisoformat(r.modified_at),
                        # "name": r.name is not None,
                        # "rank": r.site_rank is not None,
                        # "type": r.site_type is not None,
                    },
                }
            return (infile, output)

        same_as_outdir = args["output"].get_path() / "same_as"
        same_as_outdir.mkdir(parents=True, exist_ok=True)

        file2partition = {}
        for group, files in partition_files.items():
            for file in files:
                file2partition[file.path] = group

        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(read_site_data)(infile.path)
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

        # assign dedup id to each site
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
    ) -> dict[Path, InputFile]:
        outdir = args["output"].get_path()

        merged_outdir = outdir / "merged"
        merged_outdir.mkdir(parents=True, exist_ok=True)

        entity_dir = args["entity_dir"].get_path()

        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(MergeFn.exec)(
                self.workdir,
                entity_dir=entity_dir,
                infiles=infiles,
                sameas_file=same_as_files[out_relpath],
                outfile=merged_outdir
                / out_relpath.parent
                / f"{out_relpath.name}.json{COMPRESSION}",
            )
            for out_relpath, infiles in partition_files.items()
        )
        merged_outfiles: set[Path] = set()
        for file in tqdm(
            it,
            total=len(partition_files),
            desc="Merging the data",
            disable=self.verbose < 1,
        ):
            merged_outfiles.add(file)
        self.remove_unknown_files(merged_outfiles, merged_outdir)

        output: dict[Path, InputFile] = {}
        for outfile in merged_outfiles:
            output[
                outfile.relative_to(merged_outdir).parent / outfile.name.split(".")[0]
            ] = InputFile.from_relpath(
                RelPath(
                    basetype=args["output"].basetype,
                    basepath=args["output"].basepath,
                    relpath=str(outfile.relative_to(args["output"].basepath)),
                )
            )
        return output

    def prep_kg_input(
        self,
        args: MineralSiteETLServiceInvokeArgs,
        merge_files: dict[Path, InputFile],
    ):
        kg_outdir = args["output"].get_path() / "kg"
        kg_outdir.mkdir(parents=True, exist_ok=True)
        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(ExportTTLFn.exec)(
                self.workdir,
                infile=infile,
                outfile=kg_outdir / out_relpath.parent / f"{out_relpath.name}.ttl",
            )
            for out_relpath, infile in merge_files.items()
        )

        kg_outfiles = set()
        for file in tqdm(
            it,
            total=len(merge_files),
            desc="Exporting TTL",
            disable=self.verbose < 1,
        ):
            kg_outfiles.add(file)
        self.remove_unknown_files(kg_outfiles, kg_outdir)

    def prep_kgrel_input(
        self, args: MineralSiteETLServiceInvokeArgs, merge_files: dict[Path, InputFile]
    ):
        kgrel_outdir = args["output"].get_path() / "kgrel"
        kgrel_outdir.mkdir(parents=True, exist_ok=True)

        MapDedupSiteResult = TypedDict(
            "MapDedupSiteResult",
            {
                "DedupMineralSite": list[DedupMineralSite],
                "MineralSiteAndInventory": list[MineralSiteAndInventory],
            },
        )

        def map_dedup_site(file: Path) -> MapDedupSiteResult:
            sites = [
                MineralSiteAndInventory.from_dict(d) for d in serde.json.deser(file)
            ]
            dedup_sites = [
                DedupMineralSite.from_sites(sites)
                for dedup_id, sites in group_by(
                    sites, lambda site: site.ms.dedup_site_id
                ).items()
            ]

            return {"DedupMineralSite": dedup_sites, "MineralSiteAndInventory": sites}

        it: Iterable[MapDedupSiteResult] = get_parallel_executor(self.parallel)(
            delayed(map_dedup_site)(infile.path) for infile in merge_files.values()
        )  # type: ignore

        # merge all the results
        dedup_sites: dict[InternalID, list[DedupMineralSite]] = defaultdict(list)
        sites: list[MineralSiteAndInventory] = []
        for d in tqdm(
            it,
            total=len(merge_files),
            desc="Reading KGRel input",
            disable=self.verbose < 1,
        ):
            for dms in d["DedupMineralSite"]:
                dedup_sites[dms.id].append(dms)
            sites.extend(d["MineralSiteAndInventory"])

        output_dedup_sites = []
        output_sites = []
        output_inventories = []
        for lst in dedup_sites.values():
            dedup_site = DedupMineralSite.from_dedup_sites(lst, is_site_ranked=True)
            output_dedup_sites.append(dedup_site.to_dict())

        for msi in sites:
            output_sites.append(msi.ms.to_dict())
            output_inventories.append({"invs": msi.invs, "site": msi.ms.site_id})

        serde.json.ser(
            {
                "DedupMineralSite": output_dedup_sites,
                "MineralSite": output_sites,
                "MineralInventoryView": output_inventories,
            },
            kgrel_outdir / ("dedup_sites.json" + COMPRESSION),
        )


class PartitionFn:
    """Partition the mineral sites from a single file into <source_id>/<bucket>/<file_name>.json"""

    instances = {}
    num_buckets = 64

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

        outfile_subpath = infile.relpath.replace("/", "__") + COMPRESSION

        outfiles: list[Path] = []
        source2records = {
            source_id: [[] for _ in range(self.num_buckets)] for source_id in source_ids
        }
        for r in lst:
            # skip a record is fine
            r["record_id"] = str(r["record_id"]).strip()
            bucketno = PartitionFn.get_bucket_no(r["record_id"])
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

    @staticmethod
    def get_bucket_no(record_id: str) -> int:
        enc_record_id = slugify(str(record_id).strip()).encode()
        bucketno = xxhash.xxh64(enc_record_id).intdigest() % PartitionFn.num_buckets
        return bucketno

    @staticmethod
    def get_filename(username: str, source_name: str, bucket_no: int) -> Path:
        return Path(f"{username}/{source_name}/b{bucket_no:03d}.json{COMPRESSION}")


class MergeFn:
    """Merge multiple files of the same source into a single file"""

    instances = {}

    def __init__(
        self,
        workdir: Path,
        entity_dir: Path,
    ):
        self.workdir = workdir
        self.entity_service = FileEntityService(entity_dir)

    @staticmethod
    def get_instance(
        workdir: Path,
        entity_dir: Path,
    ) -> MergeFn:
        if workdir not in MergeFn.instances:
            MergeFn.instances[workdir] = MergeFn(workdir, entity_dir)
        return MergeFn.instances[workdir]

    @classmethod
    def exec(
        cls,
        workdir: Path,
        entity_dir: Path,
        **kwargs,
    ) -> Path:
        return cls.get_instance(workdir, entity_dir).invoke(**kwargs)

    @cache(
        backend=FileSqliteBackend.factory(filename="merge-v104.sqlite"),
        cache_ser_args={
            "infiles": lambda lst: orjson.dumps(
                sorted(x.get_ident() for x in lst)
            ).decode(),
            "sameas_file": lambda x: x.get_ident(),
        },
    )
    def invoke(
        self, infiles: list[InputFile], sameas_file: InputFile, outfile: Path
    ) -> Path:
        rid2sites = defaultdict(list)
        for infile in sorted(infiles, key=lambda x: x.path):
            for r in serde.json.deser(infile.path):
                rid2sites[slugify(r["record_id"])].append(r)

        dedup_map = {
            r["site_id"]: r["dedup_id"] for r in serde.json.deser(sameas_file.path)
        }

        outfile.parent.mkdir(parents=True, exist_ok=True)
        # merge the data
        output = []
        for raw_sites in rid2sites.values():
            site = MineralSite.from_dict(raw_sites[0])
            for raw_site in raw_sites[1:]:
                site.merge_mut(MineralSite.from_dict(raw_site))

            norm_site = MineralSiteAndInventory.from_raw_site(
                site.to_dict(),
                commodity_form_conversion=self.entity_service.get_commodity_form_conversion(),
                crs_names=self.entity_service.get_crs_name(),
                source_score=self.entity_service.get_data_source_score(),
            )
            norm_site.ms.dedup_site_id = dedup_map[norm_site.ms.site_id]
            output.append(norm_site.to_dict())

        serde.json.ser(output, outfile)
        return outfile


class ExportTTLFn:
    instances = {}

    def __init__(self, workdir: Path):
        self.workdir = workdir

    @staticmethod
    def get_instance(workdir: Path) -> PartitionFn:
        if workdir not in ExportTTLFn.instances:
            ExportTTLFn.instances[workdir] = ExportTTLFn(workdir)
        return ExportTTLFn.instances[workdir]

    @classmethod
    def exec(cls, workdir: Path, **kwargs) -> list[Path]:
        return cls.get_instance(workdir).invoke(**kwargs)

    @cache(
        backend=FileSqliteBackend.factory(filename="export-v100.sqlite"),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outfile: Path) -> Path:
        output = []
        for d in serde.json.deser(infile.path):
            msi = MineralSiteAndInventory.from_dict(d)
            output.extend(msi.ms.to_kg().to_triples())

        outfile.parent.mkdir(parents=True, exist_ok=True)
        with open(outfile, "w") as f:
            f.write(MINMOD_KG.prefix_part)
            f.write("\n")
            for triple in output:
                f.write(f" ".join(triple))
                f.write(". \n")
        return outfile

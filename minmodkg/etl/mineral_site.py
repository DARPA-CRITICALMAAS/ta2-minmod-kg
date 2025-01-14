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
from minmodkg.models.base import MINMOD_NS
from minmodkg.models.source import Source
from minmodkg.models_v2.inputs.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.dedup_mineral_site import DedupMineralSite
from minmodkg.models_v2.kgrel.mineral_site import MineralSite as RelMineralSite
from minmodkg.models_v2.kgrel.mineral_site import MineralSiteAndInventory
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
    source_file: RelPath
    material_form_file: RelPath
    epsg_file: RelPath
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
        with timer.watch_and_report("Deduping the data"):
            dedup_output = self.dedup(args, partition_output)
        with timer.watch_and_report("Merging the data"):
            merge_output = self.merge(args, partition_output, dedup_output)
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
    ):
        outdir = args["output"].get_path()

        merged_outdir = outdir / "merged"
        merged_outdir.mkdir(parents=True, exist_ok=True)

        material_form_file = args["material_form_file"].get_path()
        epsg_file = args["epsg_file"].get_path()
        source_file = args["source_file"].get_path()

        it: Iterable = get_parallel_executor(self.parallel)(
            delayed(MergeFn.exec)(
                self.workdir,
                material_form_file=material_form_file,
                epsg_file=epsg_file,
                source_file=source_file,
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

        output: list[InputFile] = []
        for outfile in merged_outfiles:
            output.append(
                InputFile.from_relpath(
                    RelPath(
                        basetype=args["output"].basetype,
                        basepath=args["output"].basepath,
                        relpath=str(outfile.relative_to(args["output"].basepath)),
                    )
                )
            )
        return output

    def prep_kgrel_input(
        self, args: MineralSiteETLServiceInvokeArgs, infiles: list[InputFile]
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
            delayed(map_dedup_site)(infile.path) for infile in infiles
        )  # type: ignore

        # merge all the results
        dedup_sites: dict[InternalID, list[DedupMineralSite]] = defaultdict(list)
        sites: list[MineralSiteAndInventory] = []
        for d in tqdm(
            it, total=len(infiles), desc="Reading KGRel input", disable=self.verbose < 1
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

        outfile_subpath = infile.relpath.replace("/", "__") + COMPRESSION

        outfiles: list[Path] = []
        source2records = {
            source_id: [[] for _ in range(self.num_buckets)] for source_id in source_ids
        }
        for r in lst:
            # skip a record is fine
            r["record_id"] = str(r["record_id"]).strip()
            record_id = slugify(str(r["record_id"])).encode()
            bucketno = xxhash.xxh64(record_id).intdigest() % self.num_buckets
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

    def __init__(
        self,
        workdir: Path,
        material_form_file: Path,
        epsg_file: Path,
        source_file: Path,
    ):
        self.workdir = workdir

        g = Graph()
        g.parse(material_form_file, format="ttl")
        self.material_form_conversion = {
            str(subj): float(obj.value)  # type: ignore
            for subj, obj in g.subject_objects(MINMOD_NS.mo.uri("conversion"))
        }

        g = Graph()
        g.parse(epsg_file, format="ttl")
        self.epsg_name = {
            str(subj): str(obj.value)  # type: ignore
            for subj, obj in g.subject_objects(RDFS.label)
        }

        self.source_score = {
            (s := Source.from_dict(d)).uri: s.score
            for d in serde.json.deser(source_file)
        }

    @staticmethod
    def get_instance(
        workdir: Path, material_form_file: Path, epsg_file: Path, source_file: Path
    ) -> MergeFn:
        if workdir not in MergeFn.instances:
            MergeFn.instances[workdir] = MergeFn(
                workdir, material_form_file, epsg_file, source_file
            )
        return MergeFn.instances[workdir]

    @classmethod
    def exec(
        cls,
        workdir: Path,
        material_form_file: Path,
        epsg_file: Path,
        source_file: Path,
        **kwargs,
    ) -> Path:
        return cls.get_instance(
            workdir, material_form_file, epsg_file, source_file
        ).invoke(**kwargs)

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
        self, infiles: list[InputFile], sameas_file: InputFile, outdir: Path
    ) -> Path:
        rid2sites = defaultdict(list)
        for infile in sorted(infiles, key=lambda x: x.path):
            for r in serde.json.deser(infile.path):
                rid2sites[slugify(r["record_id"])].append(r)

        dedup_map = {
            r["site_id"]: r["dedup_id"] for r in serde.json.deser(sameas_file.path)
        }

        outdir.mkdir(parents=True, exist_ok=True)
        outfile = outdir / ("merged.json" + COMPRESSION)

        # merge the data
        output = []
        for raw_sites in rid2sites.values():
            site = MineralSite.from_dict(raw_sites[0])
            for raw_site in raw_sites[1:]:
                site.merge_mut(MineralSite.from_dict(raw_site))

            norm_site = MineralSiteAndInventory.from_raw_site(
                site.to_dict(),
                self.material_form_conversion,
                self.epsg_name,
                self.source_score,
            )
            norm_site.ms.dedup_site_id = dedup_map[norm_site.ms.site_id]
            output.append(norm_site.to_dict())

        serde.json.ser(output, outfile)
        return outfile

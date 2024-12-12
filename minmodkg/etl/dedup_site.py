from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import serde.csv
import serde.json
import serde.pickle
from joblib import Parallel, delayed
from libactor.cache import cache
from minmodkg.models.base import MINMOD_KG, MINMOD_NS
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.derived_mineral_site import DerivedMineralSite
from minmodkg.models.mineral_site import MineralSite
from minmodkg.typing import InternalID
from rdflib import RDFS, Graph
from timer import Timer
from tqdm import tqdm

from statickg.helper import FileSqliteBackend
from statickg.models.etl import ETLOutput
from statickg.models.file_and_path import (
    FormatOutputPath,
    FormatOutputPathModel,
    InputFile,
    RelPath,
)
from statickg.models.repository import Repository
from statickg.services.interface import BaseFileService, BaseService


class DedupSiteServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class DedupSiteServiceInvokeArgs(TypedDict):
    predefined_entities: RelPath
    same_as_group: RelPath
    input: RelPath | list[RelPath]
    output: RelPath | FormatOutputPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class DedupSiteService(BaseFileService[DedupSiteServiceInvokeArgs]):
    """Precompute the grade/tonnage data for each site.

    Then, we will use the
    """

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: DedupSiteServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)
        self.parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

    def forward(
        self, repo: Repository, args: DedupSiteServiceInvokeArgs, output: ETLOutput
    ):
        timer = Timer()

        with timer.watch("[dedup] compute site info"):
            outfiles = self.step1_compute_site_info(repo, args)

        with timer.watch("[dedup] gen dedup site"):
            dedup_sites, sites = self.step2_gen_dedup_site(outfiles, args)

        with timer.watch("[dedup] save dedup site"):
            self.step3_save_dedup_site(dedup_sites, sites, args)

        timer.report()

    def step1_compute_site_info(
        self, repo: Repository, args: DedupSiteServiceInvokeArgs
    ):
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "step_1"
        output_fmter.outdir.mkdir(parents=True, exist_ok=True)

        predefined_entities = args["predefined_entities"].get_path()

        jobs = []
        for infile in infiles:
            outfile = output_fmter.get_outfile(infile.path)
            outfile.parent.mkdir(parents=True, exist_ok=True)
            jobs.append(
                (
                    infile,
                    outfile,
                )
            )

        readable_ptns = self.get_readable_patterns(args["input"])

        if self.parallel:
            it: Iterable = self.parallel_executor(
                delayed(ComputingDerivedSiteInfo.exec)(
                    self.workdir,
                    predefined_entities,
                    infile=infile,
                    outfile=outfile,
                )
                for infile, outfile in jobs
            )
        else:
            it: Iterable = (
                ComputingDerivedSiteInfo.exec(
                    self.workdir,
                    predefined_entities,
                    infile=infile,
                    outfile=outfile,
                )
                for infile, outfile in jobs
            )

        outfiles = set()
        for outfile in tqdm(
            it,
            total=len(jobs),
            desc=f"Precompute site info for {readable_ptns}",
            disable=self.verbose < 1,
        ):
            outfiles.add(outfile.relative_to(output_fmter.outdir))

        self.remove_unknown_files(outfiles, output_fmter.outdir)
        return [output_fmter.outdir / outfile for outfile in outfiles]

    def step2_gen_dedup_site(
        self, infiles: list[Path], args: DedupSiteServiceInvokeArgs
    ):
        sites: dict[InternalID, DerivedMineralSite] = {}
        for infile in sorted(infiles):
            for raw_derived_site in serde.json.deser(
                infile,
            ):
                site = DerivedMineralSite.from_dict(raw_derived_site)
                if site.id not in sites:
                    sites[site.id] = site
                else:
                    sites[site.id].merge(site)

        # generate dedup mineral site
        groups: list[list[InternalID]] = serde.json.deser(
            args["same_as_group"].get_path()
        )
        linked_sites = {site for grp in groups for site in grp}
        # add sites that are not linked first
        for site_id, site in sites.items():
            if site_id not in linked_sites:
                groups.append([site_id])

        # make dedup sites
        dedup_sites = [
            DedupMineralSite.from_derived_sites([sites[site_id] for site_id in grp])
            for grp in groups
        ]
        return dedup_sites, sites

    def step3_save_dedup_site(
        self,
        dedup_sites: list[DedupMineralSite],
        sites: dict[InternalID, DerivedMineralSite],
        args: DedupSiteServiceInvokeArgs,
    ):
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "final"
        output_fmter.outdir.mkdir(parents=True, exist_ok=True)

        mr = MINMOD_KG.ns.mr
        md = MINMOD_KG.ns.md
        dedup_site_pred = md.dedup_site

        with open(output_fmter.outdir / "dedup_sites.ttl", "w") as f:
            f.write(MINMOD_KG.prefix_part)
            for site in sites.values():
                for s, p, o in site.to_triples():
                    f.write(f"{s} {p} {o} .\n")

            for dedup_site in dedup_sites:
                dedup_site_uri = md[dedup_site.id]
                for site in dedup_site.sites:
                    f.write(f"{mr[site]} {dedup_site_pred} {dedup_site_uri} .\n")
                for s, p, o in dedup_site.to_triples():
                    f.write(f"{s} {p} {o} .\n")


class ComputingDerivedSiteInfo:

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

    @classmethod
    def get_instance(cls, workdir: Path, predefined_entity_dir: Path):
        key = (workdir, predefined_entity_dir)
        if key not in cls.instances:
            cls.instances[key] = cls(workdir, predefined_entity_dir)
        return cls.instances[key]

    @classmethod
    def exec(cls, workdir: Path, predefined_entity_dir: Path, **kwargs):
        return cls.get_instance(workdir, predefined_entity_dir).invoke(**kwargs)

    @cache(
        backend=FileSqliteBackend.factory(filename="compute_fn_v104.sqlite"),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outfile: Path):
        lst = serde.json.deser(infile.path)
        output = []
        for raw_site in lst:
            output.append(
                DerivedMineralSite.from_mineral_site(
                    MineralSite.from_raw_site(raw_site),
                    self.material_form_conversion,
                    self.epsg_name,
                ).to_dict()
            )
        serde.json.ser(output, outfile)
        return outfile

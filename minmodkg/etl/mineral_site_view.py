from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import orjson
import serde.csv
import serde.json
import serde.pickle
from joblib import Parallel, delayed
from libactor.cache import cache
from minmodkg.grade_tonnage_model import GradeTonnageModel
from minmodkg.models.base import MINMOD_NS
from minmodkg.models.mineral_site import MineralSite
from minmodkg.models.views.computed_mineral_site import ComputedMineralSite
from minmodkg.typing import InternalID
from rdflib import RDFS, Graph
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
from timer import Timer
from tqdm import tqdm


class MineralSiteViewServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class MineralSiteViewServiceInvokeArgs(TypedDict):
    predefined_entities: RelPath
    same_as_group: RelPath
    input: RelPath | list[RelPath]
    output: RelPath | FormatOutputPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class MineralSiteViewService(BaseFileService[MineralSiteViewServiceConstructArgs]):
    """Precompute the grade/tonnage data for each site.

    Then, we will use the
    """

    parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: MineralSiteViewServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)

    def forward(
        self,
        repo: Repository,
        args: MineralSiteViewServiceInvokeArgs,
        output: ETLOutput,
    ):
        timer = Timer()

        with timer.watch("[site-view] compute individual site view"):
            outfiles = self.step1_compute_site_view(repo, args)

        with timer.watch("[site-view] merge site views"):
            sites = self.step2_merge_site_views(outfiles, args)

        with timer.watch("[site-view] save site view"):
            self.step3_save_site_view(sites, args)

        timer.report()

    def step1_compute_site_view(
        self, repo: Repository, args: MineralSiteViewServiceInvokeArgs
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
                delayed(ComputingSiteView.exec)(
                    self.workdir,
                    predefined_entities,
                    infile=infile,
                    outfile=outfile,
                )
                for infile, outfile in jobs
            )
        else:
            it: Iterable = (
                ComputingSiteView.exec(
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

    def step2_merge_site_views(
        self, infiles: list[Path], args: MineralSiteViewServiceInvokeArgs
    ):
        sites: dict[InternalID, ComputedMineralSite] = {}
        for infile in sorted(infiles):
            for raw_derived_site in serde.json.deser(
                infile,
            ):
                site = ComputedMineralSite.from_dict(raw_derived_site)
                if site.site_id not in sites:
                    sites[site.site_id] = site
                else:
                    sites[site.site_id].merge(site)

        # generate dedup mineral site -- remove sites that were deleted but the linking is not updated yet
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
            dedup_id = ComputedMineralSite.get_dedup_id(grp)
            for site_id in grp:
                sites[site_id].dedup_id = dedup_id

        return sites

    def step3_save_site_view(
        self,
        sites: dict[InternalID, ComputedMineralSite],
        args: MineralSiteViewServiceInvokeArgs,
    ):
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "final"
        output_fmter.outdir.mkdir(parents=True, exist_ok=True)

        with open(output_fmter.outdir / "sites.json", "wb") as f:
            f.write(orjson.dumps([site.to_dict() for site in sites.values()]))


class ComputingSiteView:
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

    @staticmethod
    def invoke_subtask(
        raw_sites: list[dict],
        material_form_conversion: dict[str, float],
        epsg_name: dict[str, str],
    ):
        return [
            ComputedMineralSite.from_mineral_site(
                MineralSite.from_raw_site(raw_site),
                material_form_conversion,
                epsg_name,
            ).to_dict()
            for raw_site in raw_sites
        ]

    @cache(
        backend=FileSqliteBackend.factory(
            filename=f"compute_mineral_site_v100_v{GradeTonnageModel.VERSION}.sqlite"
        ),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outfile: Path):
        lst = serde.json.deser(infile.path)
        for site in lst:
            site["snapshot_id"] = infile.key

        if len(lst) > 1024:
            # if the number of sites is too large, we will use parallel processing
            batch_size = 512
            it: Iterable = MineralSiteViewService.parallel_executor(
                delayed(ComputingSiteView.invoke_subtask)(
                    lst[i : i + batch_size],
                    self.material_form_conversion,
                    self.epsg_name,
                )
                for i in range(0, len(lst), batch_size)
            )
            output = list(it)
        else:
            output = self.invoke_subtask(
                lst, self.material_form_conversion, self.epsg_name
            )
        serde.json.ser(output, outfile)
        return outfile

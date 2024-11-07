from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict
from uuid import uuid4

import serde.csv
import serde.json
from joblib import Parallel, delayed
from libactor.cache import cache
from minmodkg.config import MNO_NS, MNR_NS, NS_MNO
from minmodkg.grade_tonnage_model import GradeTonnageModel, SiteGradeTonnage
from minmodkg.transformations import make_site_uri
from rdflib import OWL, RDF, Graph
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
            dedup_sites = self.step2_gen_dedup_site(outfiles, args)

        with timer.watch("[dedup] save dedup site"):
            self.step3_save_dedup_site(dedup_sites, args)

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
                delayed(ComputingSiteInfo.exec)(
                    self.workdir,
                    predefined_entities,
                    infile=infile,
                    outfile=outfile,
                )
                for infile, outfile in jobs
            )
        else:
            it: Iterable = (
                ComputingSiteInfo.exec(
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
        sites: dict[str, dict] = {}
        for infile in infiles:
            for site in serde.json.deser(
                infile,
            ):
                # merging the sites & commodity
                sid = site["id"]
                if sid not in sites:
                    sites[sid] = site
                else:
                    prev_site_comms = sites[sid]["commodities"]
                    for comm, comm_val in site["commodities"].items():
                        if (
                            comm not in prev_site_comms
                            or prev_site_comms[comm]["contained_metal"] is None
                            or (
                                comm_val["contained_metal"] is not None
                                and comm_val["contained_metal"]
                                > prev_site_comms[comm]["contained_metal"]
                            )
                        ):
                            prev_site_comms[comm] = comm_val

        # generate dedup mineral site
        groups: list[list[str]] = serde.json.deser(args["same_as_group"].get_path())
        linked_sites = {site for grp in groups for site in grp}
        dedup_sites = {}
        # make dedup sites for sites that are not linked first
        for site_id, site in sites.items():
            if site_id not in linked_sites:
                dedup_id = self.get_dedup_id([site_id])
                dedup_sites[dedup_id] = {
                    "id": dedup_id,
                    "sites": {
                        site_id: {
                            comm: comm_val
                            for comm, comm_val in sites[site_id]["commodities"].items()
                        }
                    },
                    "commodities": list(site["commodities"].keys()),
                }

        # make dedup sites for sites that are linked
        for grp in groups:
            dedup_id = self.get_dedup_id(grp)
            dedup_sites[dedup_id] = {
                "id": dedup_id,
                "sites": {
                    site_id: {
                        comm: comm_val
                        for comm, comm_val in sites[site_id]["commodities"].items()
                    }
                    for site_id in grp
                },
                "commodities": sorted(
                    {comm for site_id in grp for comm in sites[site_id]["commodities"]}
                ),
            }
        return dedup_sites

    def step3_save_dedup_site(
        self, dedup_sites: dict[str, dict], args: DedupSiteServiceInvokeArgs
    ):
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "final"
        output_fmter.outdir.mkdir(parents=True, exist_ok=True)

        with open(output_fmter.outdir / "dedup_sites.ttl", "w") as f:
            f.write(f"@prefix : <{MNR_NS}> .\n")
            f.write(f"@prefix rdf: <{str(RDF)}> .\n")
            f.write(f"@prefix mno: <{MNO_NS}> .\n")
            f.write(f"@prefix owl: <{str(OWL)}> .\n\n")

            for dedup_id, dedup_site in dedup_sites.items():
                for site_id, site_comms in dedup_site["sites"].items():
                    f.write(f":{site_id} mno:dedup_site :{dedup_id}")
                    comm_nodes = []
                    for comm, comm_val in site_comms.items():
                        gtnode_id = site_id + f"__gt__{comm}"
                        comm_nodes.append((gtnode_id, comm_val))
                        f.write(f" ;\n\t mno:grade_tonnage :{gtnode_id}")
                    f.write(" .\n")

                    for gtnode_id, comm_val in comm_nodes:
                        f.write(f":{gtnode_id} mno:commodity :{comm}")
                        if comm_val["contained_metal"] is not None:
                            f.write(
                                f" ;\n\t mno:total_contained_metal {comm_val['contained_metal']}"
                                f" ;\n\t mno:total_grade {comm_val['grade']}"
                                f" ;\n\t mno:total_tonnage {comm_val['tonnage']} .\n"
                            )
                        else:
                            f.write(" .\n")

            f.write("\n")

            for dedup_id, dedup_site in dedup_sites.items():
                f.write(f":{dedup_id} rdf:type mno:DedupMineralSite")
                for site_id in dedup_site["sites"]:
                    f.write(f";\n\t mno:site :{site_id}")
                for comm in dedup_site["commodities"]:
                    f.write(f";\n\t mno:commodity :{comm}")
                f.write(" .\n")

    def get_dedup_id(self, site_ids: list[str]):
        return "dedup_" + min(site_ids)


class ComputingSiteInfo:

    instances = {}

    def __init__(self, workdir: Path, predefined_entity_dir: Path):
        self.workdir = workdir

        g = Graph()
        g.parse(predefined_entity_dir / "material_form.ttl", format="ttl")

        self.material_form_conversion = {
            str(subj): float(obj.value)  # type: ignore
            for subj, obj in g.subject_objects(NS_MNO.conversion)
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
        backend=FileSqliteBackend.factory(filename="compute_fn_v102.sqlite"),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outfile: Path):
        lst = serde.json.deser(infile.path)
        output = []
        namespace = ""
        grade_tonnage_model = GradeTonnageModel()

        for site in lst:
            site_id = make_site_uri(site["source_id"], site["record_id"], namespace)
            invs: dict[str, list] = defaultdict(list)
            commodities = set()

            for inv in site.get("mineral_inventory", []):
                if "normalized_uri" not in inv["commodity"]:
                    continue

                commodity = inv["commodity"]["normalized_uri"]
                assert commodity.startswith(MNR_NS)
                commodity = commodity[len(MNR_NS) :]
                commodities.add(commodity)

                if (
                    "ore" not in inv
                    or "value" not in inv["ore"]
                    or "unit" not in inv["ore"]
                    or "normalized_uri" not in inv["ore"]["unit"]
                    or "grade" not in inv
                    or "value" not in inv["grade"]
                    or "unit" not in inv["grade"]
                    or "normalized_uri" not in inv["grade"]["unit"]
                    or "category" not in inv
                ):
                    continue

                categories = inv["category"]

                mi_form_conversion = None
                if "material_form" in inv and "normalized_uri" in inv["material_form"]:
                    mi_form_conversion = self.material_form_conversion[
                        inv["material_form"]["normalized_uri"]
                    ]

                invs[commodity].append(
                    GradeTonnageModel.MineralInventory(
                        id=inv,
                        date=inv.get("date"),
                        zone=inv.get("zone"),
                        category=[
                            cat["normalized_uri"]
                            for cat in categories
                            if "normalized_uri" in cat
                        ],
                        material_form_conversion=mi_form_conversion,
                        ore_value=float(inv["ore"]["value"]),
                        ore_unit=inv["ore"]["unit"]["normalized_uri"],
                        grade_value=float(inv["grade"]["value"]),
                        grade_unit=inv["grade"]["unit"]["normalized_uri"],
                    )
                )

            site_comms = {}
            for commodity, gt_invs in invs.items():
                grade_tonnage = grade_tonnage_model(gt_invs) or SiteGradeTonnage()
                if grade_tonnage.total_reserve_tonnage is not None and (
                    grade_tonnage.total_resource_tonnage is None
                    or grade_tonnage.total_reserve_tonnage
                    > grade_tonnage.total_resource_tonnage
                ):
                    total_grade = grade_tonnage.get_total_reserve_grade()
                    total_tonnage = grade_tonnage.total_reserve_tonnage
                    total_contained_metal = grade_tonnage.total_reserve_contained_metal
                else:
                    total_grade = grade_tonnage.get_total_resource_grade()
                    total_tonnage = grade_tonnage.total_resource_tonnage
                    total_contained_metal = grade_tonnage.total_resource_contained_metal

                site_comms[commodity] = {
                    "contained_metal": total_contained_metal,
                    "grade": total_grade,
                    "tonnage": total_tonnage,
                }

            for comm in commodities:
                if comm not in site_comms:
                    site_comms[comm] = {
                        "contained_metal": None,
                        "grade": None,
                        "tonnage": None,
                    }

            output.append({"id": site_id, "commodities": site_comms})

        serde.json.ser(output, outfile)
        return outfile

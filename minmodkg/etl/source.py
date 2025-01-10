from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import serde.csv
import serde.json
import serde.pickle
from joblib import Parallel, delayed
from libactor.cache import cache
from minmodkg.misc.utils import assert_isinstance
from minmodkg.models.base import MINMOD_KG, MINMOD_NS
from minmodkg.models.source import Source, SourceConfig, SourceFactory
from rdflib import RDF, Graph, URIRef
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


class SourceServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class SourceServiceInvokeArgs(TypedDict):
    predefined_entities: RelPath
    input: RelPath | list[RelPath]
    output: RelPath | FormatOutputPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class SourceService(BaseFileService[SourceServiceConstructArgs]):
    """Precompute the grade/tonnage data for each site.

    Then, we will use the
    """

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: SourceServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)
        self.parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

    def forward(
        self, repo: Repository, args: SourceServiceInvokeArgs, output: ETLOutput
    ):
        timer = Timer()

        with timer.watch("[mineral-site-source] compute sources"):
            outfiles = self.step1_compute_sources(repo, args)

        with timer.watch("[mineral-site-source] aggregate and save sources"):
            self.step2_aggregate_sources(outfiles, args)

        timer.report()

    def step1_compute_sources(self, repo: Repository, args: SourceServiceInvokeArgs):
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
                delayed(ComputingSourceInfo.exec)(
                    self.workdir,
                    predefined_entities,
                    infile=infile,
                    outfile=outfile,
                )
                for infile, outfile in jobs
            )
        else:
            it: Iterable = (
                ComputingSourceInfo.exec(
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
            desc=f"Creating sources for {readable_ptns}",
            disable=self.verbose < 1,
        ):
            outfiles.add(outfile.relative_to(output_fmter.outdir))

        self.remove_unknown_files(outfiles, output_fmter.outdir)
        return [output_fmter.outdir / outfile for outfile in outfiles]

    def step2_aggregate_sources(
        self, infiles: list[Path], args: SourceServiceInvokeArgs
    ):
        output_fmter = FormatOutputPathModel.init(args["output"])
        output_fmter.outdir = output_fmter.outdir / "final"
        output_fmter.outdir.mkdir(parents=True, exist_ok=True)

        id2source = {}
        for infile in sorted(infiles):
            id2source.update(serde.json.deser(infile))

        triples = []
        for source in id2source.values():
            triples = Source.from_dict(source).to_triples(triples)

        serde.json.ser(
            list(id2source.values()), output_fmter.outdir / "sources.json", indent=2
        )

        with open(output_fmter.outdir / "sources.ttl", "w") as f:
            f.write(MINMOD_KG.prefix_part)
            for s, p, o in triples:
                f.write(f"{s} {p} {o} .\n")


class ComputingSourceInfo:

    instances = {}

    def __init__(self, workdir: Path, predefined_entity_dir: Path):
        self.workdir = workdir

        g = Graph()
        g.parse(predefined_entity_dir / "source.ttl", format="ttl")
        self.source_config = [
            SourceConfig.from_graph(assert_isinstance(subj, URIRef), g)
            for subj in g.subjects(RDF.type, MINMOD_NS.mo.uri("SourceConfig"))
        ]
        self.source_factory = SourceFactory.from_configs(self.source_config)

    @classmethod
    def get_instance(
        cls,
        workdir: Path,
        predefined_entity_dir: Path,
    ):
        key = (workdir, predefined_entity_dir)
        if key not in cls.instances:
            cls.instances[key] = cls(workdir, predefined_entity_dir)
        return cls.instances[key]

    @classmethod
    def exec(cls, workdir: Path, predefined_entity_dir: Path, **kwargs):
        return cls.get_instance(workdir, predefined_entity_dir).invoke(**kwargs)

    @cache(
        backend=FileSqliteBackend.factory(filename="compute_source_info_v102.sqlite"),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outfile: Path):
        lst = serde.json.deser(infile.path)
        output: dict[str, Source] = {}

        for raw_site in lst:
            source_id = raw_site["source_id"]
            if source_id not in output:
                output[source_id] = self.source_factory.get_source(source_id)

        serde.json.ser({k: v.to_dict() for k, v in output.items()}, outfile)
        return outfile

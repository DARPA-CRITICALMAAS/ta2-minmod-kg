from __future__ import annotations

import hashlib
import importlib
import re
import subprocess
import sys
from dataclasses import dataclass
from functools import cached_property, lru_cache
from importlib.metadata import version
from pathlib import Path
from typing import Annotated, Callable, NotRequired, TypedDict

import orjson
import typer
from drepr.main import convert
from loguru import logger
from rdflib import RDF, SH, Graph
from tqdm.auto import tqdm

from statickg.helper import CacheProcess, import_func
from statickg.models.file_and_path import BaseType, RelPath
from statickg.models.prelude import ETLOutput, RelPath, Repository
from statickg.services.interface import BaseFileService


class FilenameValidatorServiceConstructArgs(TypedDict):
    pattern: str
    verbose: NotRequired[int]


class FilenameValidatorServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]


class FilenameValidatorService(BaseFileService[FilenameValidatorServiceInvokeArgs]):

    def forward(
        self,
        repo: Repository,
        args: FilenameValidatorServiceInvokeArgs,
        output: ETLOutput,
    ):
        verbose = self.args.get("verbose", 1)
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=False,
            optional=False,
            compute_missing_file_key=False,
        )

        regex = re.compile(self.args["pattern"])

        invalid_filenames = []
        for infile in tqdm(infiles, desc="Validate file names", disable=verbose != 1):
            if not regex.match(infile.path.stem):
                invalid_filenames.append(infile.relpath)

        if len(invalid_filenames) > 0:
            self.logger.error(
                "Invalid filenames: \n{}",
                "\n".join(f"\t{x}" for x in invalid_filenames),
            )
            raise Exception("Found invalid filenames")


class ContentValidator:

    @dataclass
    class DReprModel:
        key: str
        validate_model: Callable[[Path], None]
        transform_model: Callable[[Path], str]

    def __init__(self, workdir: Path, shacl_bin: Path, predefined_ent_dir: RelPath):
        self.workdir = workdir
        self.predefined_ent_dir = predefined_ent_dir
        self.shacl_bin = shacl_bin
        self.shape_file = Path(__file__).parent.parent / "schema/shapes.ttl"

        assert shacl_bin.exists(), f"SHACL binary not found at {shacl_bin}"

        # create additional directories
        (workdir / "mineral-sites").mkdir(parents=True, exist_ok=True)
        (workdir / "entities").mkdir(parents=True, exist_ok=True)

        self.extractor_dir = RelPath(
            basetype=BaseType.CFG_DIR,
            basepath=Path(__file__).parent.parent,
            relpath="extractors",
        )
        self.pkgdir = self.setup(workdir)
        self.drepr_version = version("drepr-v2").strip()
        self.cache = CacheProcess(workdir / "cache.db")

    def setup(self, workdir: Path):
        pkgname = "gen_validate_programs"
        pkgdir = workdir / pkgname

        try:
            m = importlib.import_module(pkgname)
            if Path(m.__path__[0]) != pkgdir:
                raise ValueError(
                    f"Existing a python package named {pkgname}, please uninstall it because it is reserved to store generated DREPR programs"
                )
        except ModuleNotFoundError:
            # we can use services as the name of the folder containing the services as it doesn't conflict with any existing
            # python packages
            pass

        pkgdir.mkdir(parents=True, exist_ok=True)
        (pkgdir / "__init__.py").touch(exist_ok=True)

        # add the package to the path
        if str(pkgdir.parent) not in sys.path:
            sys.path.insert(0, str(pkgdir.parent))

        return pkgdir

    def get_cache_key(self, filepath: RelPath):
        return f"validator:{self.drepr_version}:{hashlib.sha256(filepath.get_path().read_bytes()).hexdigest()}"

    @cached_property
    def site_model(self):
        return self._get_drepr_model("mineral_site")

    @cached_property
    def predefined_entity_models(self):
        return {
            key: self._get_drepr_model(key)
            for key in [
                "category",
                "commodity",
                "deposit_type",
                "unit",
                "country",
                "state_or_province",
                "epsg",
                "material_form",
            ]
        }

    @cached_property
    def _predefined_entity_graph(self):
        data = []
        for file in self.predefined_ent_dir.iterdir():
            if file.suffix != ".json" and file.suffix != ".csv":
                continue

            outfile = self.workdir / "entities" / (file.stem + ".ttl")
            self._exec_drepr_model(
                self.predefined_entity_models[file.stem], file, outfile
            )
            data.append(outfile.read_text())
        return "\n".join(data)

    def _get_drepr_model(self, name: str):
        model_file = self.extractor_dir / f"{name}.yml"
        model_filepath = model_file.get_path()

        model_outfile = self.pkgdir / f"{model_filepath.stem}.py"
        model_key = self.get_cache_key(model_file)

        with self.cache.auto(
            filepath=model_file.get_ident(),
            key=model_key,
        ) as notfound:
            if notfound:
                try:
                    convert(
                        repr=model_filepath,
                        resources={},
                        progfile=model_outfile,
                    )
                except:
                    logger.error(
                        "Error when generating program {}", model_file.get_ident()
                    )
                    raise
                logger.info("generate program {}", model_file.get_ident())

        return ContentValidator.DReprModel(
            key=model_key,
            validate_model=lambda x: None,
            transform_model=import_func(
                f"{model_outfile.parent.name}.{model_outfile.stem}.main"
            ),
        )

    def _exec_drepr_model(
        self, model: ContentValidator.DReprModel, infile: RelPath, outfile: Path
    ):
        with self.cache.auto(
            filepath=infile.get_ident(),
            key=self.get_cache_key(infile) + ":model:" + model.key,
            outfile=outfile,
        ) as notfound:
            if notfound:
                try:
                    output = model.transform_model(infile.get_path())
                except:
                    logger.error("Encounter error when converting data to TTL")
                    raise
                outfile.write_text(output)

    def validate_mineral_site(self, site_file: RelPath):
        logger.info("Validating file {}", site_file)
        outfile = self.workdir / "mineral-sites" / (site_file.stem + ".ttl")

        # ------- schema check - pass 1 -------
        logger.info("Check if the data is valid with D-REPR (part 1)...")

        # ------- convert the site file to ttl -------
        logger.info("Check if the data can be converted to TTL...")
        self._exec_drepr_model(self.site_model, site_file, outfile)
        logger.info("Check if the data can be converted to TTL... Success!")

        # ------- validate again using SHACL to ensure all references are correct -------
        logger.info("Check if the data is valid with SHACL (part 2)...")
        # prepare a graph
        full_graph = self.workdir / "mineral-sites" / (site_file.stem + ".full.ttl")
        with open(full_graph, "w") as f:
            f.write(self._predefined_entity_graph)
            f.write("\n")
            f.write(outfile.read_text())

        process = subprocess.run(
            [
                "bash",
                str(self.shacl_bin),
                "-datafile",
                str(full_graph),
                "-shapesfile",
                str(self.shape_file),
            ],
            stdout=subprocess.PIPE,
        )
        shacl_output = process.stdout.decode("utf-8")

        # removing warning
        shacl_output_lines = shacl_output.splitlines()
        for i, line in enumerate(shacl_output_lines):
            if not line.startswith("@prefix"):
                assert re.match(r"\d*", line) is not None
                assert "WARN" in line
            else:
                shacl_output = "\n".join(shacl_output_lines[i:])
                break

        if process.returncode != 0:
            print("=" * 80, "\n", "-" * 10, "SHACL output", "-" * 10, "\n")
            print(shacl_output)
            print("=" * 80)
            raise Exception("SHACL validation failed")
        else:
            g = Graph()
            try:
                g.parse(data=shacl_output, format="turtle")
            except:
                print("=" * 80, "\n", "-" * 10, "SHACL output", "-" * 10, "\n")
                print(shacl_output)
                print("=" * 80)
                raise Exception("SHACL validation failed")

            for subj in g.subjects(RDF.type, SH.ValidationReport):
                (conforms,) = list(g.objects(subj, SH.conforms))
                if not conforms:
                    print("=" * 80, "\n", "-" * 10, "SHACL output", "-" * 10, "\n")
                    print(shacl_output)
                    print("=" * 80)
                    raise Exception("SHACL validation failed")

        logger.info("Check if the data is valid with SHACL (part 2)... Success!")


if __name__ == "__main__":
    app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)

    @app.command()
    def validate(
        datadir: Annotated[
            Path, typer.Argument(help="A directory containing the data Git repository")
        ],
        workdir: Annotated[
            Path,
            typer.Argument(help="A directory for storing intermediate ETL results"),
        ],
        shacl_bin: Annotated[
            Path,
            typer.Argument(
                help="A path to the SHACL binary",
            ),
        ],
        changed_files_lst: Annotated[
            Path,
            typer.Argument(
                help="A list of changed files (relative path) in the data repository",
            ),
        ],
    ):
        validator = ContentValidator(
            workdir,
            shacl_bin,
            RelPath(
                basetype=BaseType.DATA_DIR, basepath=datadir, relpath="data/entities"
            ),
        )

        if changed_files_lst.name.endswith(".json"):
            changed_files = orjson.loads(changed_files_lst.read_bytes())
        else:
            assert changed_files_lst.name.endswith(".txt")
            changed_files = changed_files_lst.read_text().splitlines()

        for relpath in changed_files:
            if relpath.startswith("data/entities/"):
                # validate the entities
                raise NotImplementedError()
            elif any(
                relpath.startswith(dir)
                for dir in [
                    "data/inferlink/extractions/",
                    "data/umn/",
                    "data/sri/",
                    "data/usc/",
                    "test-mineral-sites/",
                ]
            ) and relpath.endswith(".json"):
                print("::group::Validate file", relpath)
                validator.validate_mineral_site(
                    RelPath(
                        basetype=BaseType.DATA_DIR, basepath=datadir, relpath=relpath
                    )
                )
                print("::endgroup::")
            else:
                print("::group::Not validate file", relpath)
                print("::endgroup::")

    app()

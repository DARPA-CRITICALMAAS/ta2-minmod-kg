from __future__ import annotations

import re
from typing import NotRequired, TypedDict

from tqdm.auto import tqdm

from statickg.helper import logger_helper
from statickg.models.input_file import RelPath
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
            unique_filename=False,
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

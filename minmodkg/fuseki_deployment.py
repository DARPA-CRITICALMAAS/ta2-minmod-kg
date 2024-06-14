from __future__ import annotations

import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Mapping, NotRequired, TypedDict

from loguru import logger

from statickg.helper import find_available_port, get_classpath
from statickg.models.prelude import (
    ETLOutput,
    RelPathRefStr,
    RelPathRefStrOrStr,
    Repository,
)
from statickg.services.fuseki import DBInfo, FusekiDataLoaderService
from statickg.services.interface import BaseFileService, BaseService


class FusekiDeploymentServiceConstructArgs(TypedDict):
    hostname: NotRequired[str]


class FusekiDeploymentServiceInvokeArgs(TypedDict):
    start: RelPathRefStrOrStr
    stop: RelPathRefStrOrStr


class FusekiDeploymentService(BaseService[FusekiDeploymentServiceInvokeArgs]):

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: FusekiDeploymentServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        self.name = name
        self.workdir = workdir
        self.services = services
        self.logger = logger.bind(name=get_classpath(self.__class__).rsplit(".", 1)[0])
        self.args = args
        self.hostname = args.get("hostname", "http://localhost")

    def forward(
        self,
        repo: Repository,
        args: FusekiDeploymentServiceInvokeArgs,
        agg_output: ETLOutput,
    ):
        args = deepcopy(args)

        # --------------------------------------------------------------
        # spin up a Fuseki server serving the directory
        lst: list[DBInfo] = agg_output.output[get_classpath(FusekiDataLoaderService)]
        (dbinfo,) = lst

        if not dbinfo.has_running_service():
            # only deploy the service when there is no running service for the directory
            self.start_fuseki(args, dbinfo)

        # --------------------------------------------------------------
        # after deployment, we will make sure that only one service is running
        return

    def start_fuseki(self, args: FusekiDeploymentServiceInvokeArgs, dbinfo: DBInfo):
        if dbinfo.hostname is not None:
            return

        name = f"fuseki-{dbinfo.dir.name}"
        port = find_available_port(self.hostname, 3030)
        subprocess.check_call(
            self.get_start_command(args).format(
                ID=name, PORT=str(port), DB_DIR=dbinfo.dir
            ),
            shell=True,
        )
        dbinfo.hostname = f"{self.hostname}:{port}"
        self.logger.debug(
            "Started Fuseki service at {} serving {}", dbinfo.hostname, dbinfo.dir.name
        )

    def shutdown_fuseki(self, args: FusekiDeploymentServiceInvokeArgs, dbinfo: DBInfo):
        # we should only have one service running at a time
        if dbinfo.hostname is None:
            return

        subprocess.check_call(
            self.get_stop_command(args).format(ID=f"fuseki-{dbinfo.dir.name}"),
            shell=True,
        )
        self.logger.debug(
            "Stopped Fuseki service at {}, which serves {}",
            dbinfo.hostname,
            dbinfo.dir.name,
        )
        dbinfo.hostname = None

    def get_start_command(self, args: FusekiDeploymentServiceInvokeArgs):
        cmd = args["start"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["start"] = cmd
        return cmd

    def get_stop_command(self, args: FusekiDeploymentServiceInvokeArgs):
        cmd = args["stop"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["stop"] = cmd
        return cmd

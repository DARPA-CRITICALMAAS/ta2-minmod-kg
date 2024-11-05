from __future__ import annotations

import os
import shutil
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Mapping, NotRequired, TypedDict

from loguru import logger

from statickg.helper import (
    find_available_port,
    get_classpath,
    is_port_available,
    wait_till_port_available,
)
from statickg.models.prelude import (
    ETLOutput,
    RelPathRefStr,
    RelPathRefStrOrStr,
    Repository,
)
from statickg.services.fuseki import DBInfo, FusekiDataLoaderService
from statickg.services.interface import BaseService


class FusekiDeploymentServiceConstructArgs(TypedDict):
    hostname: NotRequired[str]


class FusekiDeploymentServiceInvokeArgs(TypedDict):
    start: RelPathRefStrOrStr
    stop_all: RelPathRefStrOrStr


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
        self.current_dbinfo = None
        self.fuseki_port = int(os.environ.get("FUSEKI_PORT", "3030"))

    def forward(
        self,
        repo: Repository,
        args: FusekiDeploymentServiceInvokeArgs,
        agg_output: ETLOutput,
    ):
        args = deepcopy(args)
        lst: list[DBInfo] = agg_output.output[get_classpath(FusekiDataLoaderService)]
        (dbinfo,) = lst

        if not dbinfo.has_running_service():
            if not is_port_available(self.hostname, self.fuseki_port):
                subprocess.check_call(self.get_stop_all_command(args), shell=True)

            if not wait_till_port_available(
                self.hostname, self.fuseki_port, timeout=10
            ):
                self.logger.error(
                    f"After stopping all fuseki services, port {self.fuseki_port} is still not available."
                )
                raise Exception(
                    f"Another one started a service on port {self.fuseki_port} that is not managed by this service"
                )

            # only deploy the service when there is no running service for the directory
            if self.start_fuseki(args, dbinfo):
                # remove old databases
                for old_db in dbinfo.get_older_versions():
                    shutil.rmtree(old_db.dir)
        else:
            for old_db in dbinfo.get_older_versions():
                shutil.rmtree(old_db.dir)

        return

    def start_fuseki(self, args: FusekiDeploymentServiceInvokeArgs, dbinfo: DBInfo):
        if dbinfo.hostname is not None:
            return False

        name = f"fuseki-{dbinfo.dir.name}"
        port = find_available_port(self.hostname, self.fuseki_port)
        try:
            subprocess.check_call(
                self.get_start_command(args).format(
                    ID=name, PORT=str(port), DB_DIR=dbinfo.dir
                ),
                shell=True,
            )
        except subprocess.CalledProcessError:
            subprocess.check_call(
                self.get_stop_all_command(args).format(ID=name),
                shell=True,
            )
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
        return True

    def get_start_command(self, args: FusekiDeploymentServiceInvokeArgs):
        cmd = args["start"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["start"] = cmd
        return cmd

    def get_stop_all_command(self, args: FusekiDeploymentServiceInvokeArgs):
        cmd = args["stop_all"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["stop_all"] = cmd
        return cmd

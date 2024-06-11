#!/bin/bash

set -x

CWD=$(pwd)

# start ETL service
docker run --name etl --rm --network host \
    -w $CWD \
    -v $CWD:$CWD \
    -v /var/run/docker.sock:/var/run/docker.sock \
    minmod-etl python -m statickg ./ta2-minmod-kg/etl.yml ./kgdata ./ta2-minmod-data

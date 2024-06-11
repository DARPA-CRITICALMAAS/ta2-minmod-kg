#!/bin/bash

CWD=$(pwd)
UID=$(id -u)
GID=$(id -g)

set -x

# start ETL service
docker run --name etl --rm --network host \
    -w $CWD \
    -v $CWD:$CWD \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -u $UID:$GID \
    minmod-etl python -m statickg ./ta2-minmod-kg/etl.yml ./kgdata ./ta2-minmod-data

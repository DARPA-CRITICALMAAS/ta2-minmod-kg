#!/bin/bash

set -x

# start ETL service
docker run --name etl --rm -d \
    -v ./ta2-minmod-kg:/kg \
    -v ./data:/kg-data \
    -v ./ta2-minmod-data:/data \
    -v /var/run/docker.sock:/var/run/docker.sock \
    python -m statickg /kg/etl.yml /kg-data /data
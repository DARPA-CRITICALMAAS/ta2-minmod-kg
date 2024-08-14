# Overview

Code for TA2 Knowledge Graph and other related services such as its API, CDR integration, data browser, etc.

## Repository Structure

- [containers](/containers): scripts to build docker images to deploy TA2 KG and other related services.
- [extractors](/extractors): [d-repr](https://github.com/usc-isi-i2/d-repr) models to convert [TA2 data](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/) in to RDF to import into TA2 KG.
- [schema](/schema): the [ontology definition](/schema/ontology.ttl) and other generated files such as ER diagram and SHACL definition (for data validation)
- [tests](/tests): testing code
- [minmodkg](/minmodkg): main Python code

## Installation

### Setup the workspace

Setup the workspace by cloning [ta2-minmod-data](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data) and this repository [ta2-minmod-kg](/) inside your `WORKDIR`

```bash
git clone --depth 1 https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data
git clone --depth 1 https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg
mkdir kgdata
```

### Setup dependencies

* Install required services using Docker:
  
    List of services: [graph database](https://jena.apache.org/documentation/fuseki2/) and our [API](/minmodkg/api.py)

    ```
    cd ta2-minmod-kg
    USER_ID=$(id -u) GROUP_ID=$(id -g) docker-compose build
    cd ..
    ```
    
    On mac, use `docker compose` instead of `docker-compose`

    ```
    cd ta2-minmod-kg
    USER_ID=$(id -u) GROUP_ID=$(id -g) docker compose build
    cd ..
    ```
  
* Install python library

    Require [poetry](https://python-poetry.org/): `pip install poetry`
    ```
    cd ta2-minmod-kg
    python -m venv .venv
    poetry install --only main
    cd ..
    ```

## Quick start

1. Start the process that build TA2 graph database:

```
source ta2-minmod-kg/.venv/bin/activate
python -m statickg ta2-minmod-kg/etl.yml ./kgdata ta2-minmod-data --overwrite-config --refresh 20
```

2. Start API and other services

```
docker-compose up nginx api
```

3. Upload data to CDR:

```
export CDR_TOKEN=<your cdr token>
python -m minmodkg.integrations.cdr
```

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

**1. Installing required services using Docker:**
  
    List of services: [graph database](https://jena.apache.org/documentation/fuseki2/), our [API](/minmodkg/api.py), [nginx](https://nginx.org)

    ```
    cd ta2-minmod-kg
    USER_ID=$(id -u) GROUP_ID=$(id -g) docker compose build
    cd ..
    ```
  
**2. Installing python library**

    Requiring [poetry](https://python-poetry.org/): `pip install poetry`
    ```
    cd ta2-minmod-kg
    python -m venv .venv
    poetry install --only main
    cd ..
    ```

**3. Generating an SSL certificate**

You can use [Let's Encrypt](https://letsencrypt.org/) to create a free SSL certificate for your server. However, for the purpose of testing locally, you can generate your own using the following command

```
openssl req -x509 -newkey rsa:4096 -keyout privkey.pem -out fullchain.pem -sha256 -days 3650 -nodes -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=CommonNameOrHostname"
```

## Usage

**1. Building TA2 graph database**

```
source ta2-minmod-kg/.venv/bin/activate
python -m statickg ta2-minmod-kg/etl.yml ./kgdata ta2-minmod-data --overwrite-config --refresh 20
```

Note that this process will continue running to monitor for new changes. Hence, it will not terminate unless a terminating signal is received explicitly.

**2. Starting other services**

Let `CERT_PATH` be the directory containing your SSL certificate (`fullchain.pem` and `privkey.pem`) created in the [setup step](#installation).

```
cd ta2-minmod-kg
CERT_DIR=<CERT_PATH> docker compose up nginx api
```

If you also want to start our [dashboard](https://minmod.isi.edu), run `CERT_DIR=<CERT_PATH> docker compose up nginx api dashboard` instead. Note that currently, URLs for TA2 services are hardcoded in the dashboard, so it will not query our local services.

Once it starts, you can view our API docs in [https://localhost/api/v1/docs](https://localhost/api/v1/docs)

**3. Upload data to CDR**

To upload data to CDR, you need to obtain a token first. Then, run the following command:

```
export CDR_TOKEN=<your cdr token>
export MINMOD_API=https://localhost/api/v1
export MINMOD_SYSTEM=test
python -m minmodkg.integrations.cdr
```

The two environment variables `MINMOD_API` and `MINMOD_SYSTEM` are used for testing purposes. For production, you can simply ignore these two variables.

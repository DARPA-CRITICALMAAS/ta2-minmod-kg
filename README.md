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

List of services: [knowledge graph](https://jena.apache.org/documentation/fuseki2/), our [API](/minmodkg/api.py), [nginx](https://nginx.org)

```
cd ta2-minmod-kg
docker network create minmod
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

<div align="center">
  <img src="/docs/system-overview.png" alt="drawing" width="70%" />
</div>

The figure above denotes how different components work to produce mineral site data and grade tonnage models. The rounded rectangle are systems, blue rectangle are processes, and the parallelogram is the input source.

**1. Building TA2 knowledge graph**

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

The two environment variables `MINMOD_API` and `MINMOD_SYSTEM` are used for testing purposes. For production, you can simply ignore these two variables and our code will use the default values `MINMOD_API=https://minmod.isi.edu/api/v1` and `MINMOD_SYSTEM=minmod`.

**4. Download data from CDR**

The CDR endpoint for getting TA2 output is [https://api.cdr.land/docs#/Minerals/list_dedup_site_commodity_v1_minerals_dedup_site_search__commodity__get](https://api.cdr.land/docs#/Minerals/list_dedup_site_commodity_v1_minerals_dedup_site_search__commodity__get).

There are two required parameters:
- commodity: name of the commodity (capitalize, case-sensitive) e.g., `Lithium`. Here is [list of commodities](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/blob/main/data/entities/commodity.csv)
- system: `minmod`

By default, the CDR should return all sites matched the provided two parameters. To further filter the data for sites with deposit type classification and sites with grade/tonnage data, we can apply the following additional filters:
- Site with deposit type classification: `with_location = true`, and `with_deposit_types_only = true`

  CURL Command:
  ```
  curl -X 'GET' \
  'https://api.cdr.land/v1/minerals/dedup-site/search/Lithium?with_location=true&with_deposit_types_only=true&system=minmod&top_n=1&limit=-1' \
  -H 'Authorization: Bearer <your token>'
  ```
- Site with grade/tonnage data: `with_contained_metals = true`

  CURL Command:
  ```
  curl -X 'GET' \
  'https://api.cdr.land/v1/minerals/dedup-site/search/Lithium?with_contained_metals=true&system=minmod&top_n=1&limit=-1' \
  -H 'Authorization: Bearer <your token>'
  ```

**5. Browsing the data locally**

You can browse the data locally by replacing the hostname from `minmod.isi.edu` to `localhost. For example, `https://minmod.isi.edu/resource/kg` by `https://localhost/resource/kg`

**6. Querying data**

If you know [SPARQL](https://en.wikipedia.org/wiki/SPARQL), you can query the data by sending your query to `https://<hostname>/sparql`. We have a helper function to make it easier if you know Python in [here](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg/blob/04622ff8220a543f84f5207b9d9b1d9f95036888/minmodkg/misc.py#L61)

Also, you can download mineral site data and grade/tonnage model directly from our API. Please see our API docs in [https://localhost/api/v1/docs](https://localhost/api/v1/docs) for more information.

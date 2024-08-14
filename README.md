# Overview

Code for TA2 Knowledge Graph and other related services such as its API, CDR integration, data browser, etc.

## Repository Structure

- [containers](/containers): scripts to build docker images to deploy TA2 KG and other related services.
- [extractors](/extractors): [d-repr](https://github.com/usc-isi-i2/d-repr) models to convert [TA2 data](https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/) in to RDF to import into TA2 KG.
- [schema](/schema): the [ontology definition](/schema/ontology.ttl) and other generated files such as ER diagram and SHACL definition (for data validation)
- [tests](/tests): testing code
- [minmodkg](/minmodkg): main Python code

## Installation

List of providing services:

1. Graph database ([Fuseki](https://jena.apache.org/documentation/fuseki2/)): for processing SPARQL queries
2. [API](/minmodkg/api.py)
3. Nginx

### Docker

Building required docker images: `USER_ID=$(id -u) GROUP_ID=$(id -g) docker-compose build`

### Manual

`poetry install`

## Usage




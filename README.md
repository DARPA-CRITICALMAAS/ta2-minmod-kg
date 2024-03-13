# ta2-minmod-kg
code repository for TA2 Knowledge Graph (MinMod) construction &amp; deployment

## Repository Structure

- `/validators` (https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg/tree/main/validators): 
  - This directory holds python scripts that are used for validating the format of json and ttl files before the data gets added to a knowledge graph
- `/generator` (https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg/tree/main/generator):
    - This directory holds python scripts that are used for generating ttl files that are used for adding data to knowledge graph
- `/deployment` (https://github.com/DARPA-CRITICALMAAS/ta2-minmod-kg/tree/main/deployment):
    - This directory contains the script that runs on a virtual machine and deploys the generated ttl files to SPARQL endpoint

docker run --rm -it -p 3030:3030 -v /workspace/darpa-criticalmaas/ta2-minmod-kg/data:/data minmod-fuseki bash

/opt/jena/bin/tdb2.tdbloader --loc /database */*.ttl
#!/bin/bash

set -ex

# Start Blazegraph
java -server $JVM_ARGS -Dbigdata.propertyFile=/home/criticalmaas/config/blazegraph.properties -jar /home/criticalmaas/blazegraph.jar
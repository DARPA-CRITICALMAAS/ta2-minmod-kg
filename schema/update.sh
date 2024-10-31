#!/bin/bash

set -ex

python -m minmodkg.schema.make_diagram
python -m minmodkg.schema.make_shapes
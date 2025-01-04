from __future__ import annotations

import shutil
import subprocess
import time

"""Load files into the database"""

import os
import sys
from pathlib import Path


def main(input_relpaths: Path):
    indir = input_relpaths.parent
    outdir = Path("/home/criticalmaas/dataloader")
    if outdir.exists():
        shutil.rmtree(outdir)

    print("Preparing input files...", end=" ", flush=True)
    start = time.time()
    for file in input_relpaths.read_text().splitlines():
        (outdir / file).parent.mkdir(exist_ok=True, parents=True)
        os.symlink(indir / file, outdir / file)
    print(f"Done in {time.time() - start:.2f}s")

    print("Loading files into the database...", end=" ", flush=True)
    start = time.time()

    subprocess.check_call(
        [
            "java",
            "-Xmx6g",
            "-cp",
            "/home/criticalmaas/bigdata.jar",
            "com.bigdata.rdf.store.DataLoader",
            "-verbose",
            "/home/criticalmaas/config/blazegraph.properties",
            "/home/criticalmaas/dataloader",
        ]
    )

    print(f"Done in {time.time() - start:.2f}s")


if __name__ == "__main__":
    main(Path(sys.argv[1]))

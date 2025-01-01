from __future__ import annotations

import shutil
import subprocess
import time

"""Load files into the database"""

import os
import sys
from pathlib import Path

import jaydebeapi


def retry_connect(
    connection_url: str, *, retry_after: float = 0.2, max_wait_seconds: float = 30
):
    print("Connecting to the database", end="", flush=True)
    conn = None
    for _ in range(int(max_wait_seconds / retry_after)):
        try:
            conn = jaydebeapi.connect(
                "virtuoso.jdbc4.Driver",
                connection_url,
            )
        except Exception:
            print(".", end="", flush=True)
            time.sleep(retry_after)

    if conn is None:
        print("Failed to connect to the database!")
        sys.exit(1)
    print("Connected!")
    return conn


def main(input_relpaths: Path):
    indir = input_relpaths.parent
    outdir = Path("/criticalmaas/dataloader")
    shutil.rmtree(outdir)

    print("Preparing input files...", end=" ", flush=True)
    start = time.time()
    for file in input_relpaths.read_text().splitlines():
        (outdir / file).parent.mkdir(exist_ok=True, parents=True)
        os.symlink(indir / file, outdir / file)
    print(f"Done in {time.time() - start:.2f}s")

    conn = retry_connect(
        f"jdbc:virtuoso://localhost:1111/UID=dba/PWD=dba",
    )

    print("Loading files into the database...", end=" ", flush=True)
    start = time.time()

    curs = conn.cursor()
    curs.execute(
        f"ld_dir_all('{outdir.absolute()}', '*.ttl', 'https://minmod.isi.edu')"
    )
    curs.execute("rdf_loader_run()")
    print(f"Done in {time.time() - start:.2f}s")

    print("Verifying the results...", end=" ", flush=True)
    start = time.time()
    curs.execute("SELECT ll_file, ll_state, ll_error FROM DB.DBA.load_list")
    load_results = curs.fetchall()
    assert all(
        ll_state == 2 for _, ll_state, _ in load_results
    ), "rdf_loader_run must be blocking until all files are loaded or failed"
    load_results = [
        (ll_file, ll_state, ll_error)
        for ll_file, ll_state, ll_error in load_results
        if ll_error is not None
    ]

    curs.execute("checkpoint")
    curs.execute("checkpoint_interval(60)")
    curs.execute("scheduler_interval(10)")

    print(f"Done in {time.time() - start:.2f}s")
    if len(load_results) > 0:
        print("Discovered the following errors:")
        for ll_file, ll_state, ll_error in load_results:
            print("- File:", Path(ll_file).relative_to(outdir), "error:", ll_error)
        sys.exit(1)
    else:
        print("All files are loaded successfully!")


if __name__ == "__main__":
    main(Path(sys.argv[1]))

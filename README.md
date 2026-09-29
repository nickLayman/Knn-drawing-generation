# Complete bipartite drawing census

This repository reproduces the census of good drawings of complete bipartite
graphs on at most eight vertices. It starts from the two hard-coded drawings of
`K(2,2)`, adds one vertex and all its incident edges at a time, and removes
equivalent drawings after each completed vertex block.

The program retains the final strong drawing representatives and converts each
one to both flag representations used in the crossing-number calculation:

- crossing-pair flags record which pairs of graph edges cross;
- 4-graph flags record the four endpoints of each crossing pair.

The conversion is included here. Exact reduction of those rows modulo flag
isomorphism uses Bernard Lidicky's `flag.cpp`, which is an explicit external
dependency and is not distributed in this repository.

## Requirements

- Linux;
- Python 3.12 or newer;
- enough memory and storage for the selected graph;
- optionally, GNU `sort`, `g++`, and a separate copy of `flag.cpp` for exact
  flag-isomorphism reduction.

The drawing generator itself uses only the Python standard library.

### Why Linux is currently required

Linux is a current implementation requirement rather than a mathematical one.
The coordinator protocol, SQLite database, drawing representation, and flag
formats are portable. The following operating-system assumptions still occur
in the executable paths:

| Location | Current Linux assumption | Change needed on macOS | Change needed on Windows |
|---|---|---|---|
| `src/graph_drawings/local_runner.py`: `run_local`, `_run_coordinator`, and `_run_worker` | Child processes inherit the configured output and scratch roots. This follows Linux's `fork` behavior. | macOS uses `spawn` by default. Pass the runtime roots to both child entry points and call `config.configure_runtime` in each child, or create all processes from an explicitly initialized multiprocessing context. | Make the same spawn-safe change. Keep process creation behind the existing `if __name__ == "__main__"` entry point and add `multiprocessing.freeze_support()` if a frozen executable is distributed. |
| `src/graph_drawings/cli.py`: `export_all_shards` and `reduce_variant` | Both pools explicitly request `multiprocessing.get_context("fork")`. | Replace `fork` with `spawn` and confirm that the existing export initializer supplies every worker global. The reduction tasks already carry their paths as arguments. | Make the same replacement; `fork` is unavailable on Windows. Test all pool arguments under Python's pickle rules. |
| `src/graph_drawings/cli.py`: `usage_snapshot` and `export_shard`; `src/graph_drawings/worker.py`: the two `resource.getrusage` calls | Python's Unix-only `resource` module supplies process CPU time and maximum RSS. Linux reports `ru_maxrss` in KiB. | The module exists, but macOS reports `ru_maxrss` in bytes. Divide that value by 1024 before recording the fields named `*_kib`. | Replace these calls with a Windows-capable source such as `psutil`, or make RSS diagnostics optional and use `time.process_time()` for CPU time. Imports of `resource` must also become conditional. |
| `src/graph_drawings/cli.py`: `sort_fixed_rows` | Exact flag reduction invokes GNU `sort` with `--parallel`, `-S`, `-T`, `-o`, and `LC_ALL=C`. | Install GNU coreutils and invoke `gsort`, adding a `--sort-command` option, or replace this step with a Python external merge sort. The BSD `sort` shipped with macOS does not accept the full command used here. | Supply GNU `sort` through MSYS2/MinGW and select it explicitly, or implement the external merge sort in Python. Windows `sort.exe` is not compatible. |
| `src/graph_drawings/cli.py`: `reduce_bucket` and `parse_time_report` | If `/usr/bin/time` exists, the reducer uses GNU `time -v` and parses its verbose field names. | Do not select `/usr/bin/time` merely because the path exists: the macOS program has a different interface. Detect GNU time (commonly installed as `gtime`), add a configurable command, or omit these optional measurements. | Skip this wrapper or collect the same optional measurements through `psutil`; `/usr/bin/time` is normally absent. |
| `src/graph_drawings/cli.py`: `compile_reducer` and `parse_args` | The external `flag.cpp` source is compiled with a hard-coded `g++ -O3 -std=c++17 ...` command. | Accept a `--cxx` command and use `clang++` or an installed `g++`; the remaining preprocessor definitions and source arguments can stay the same. | The shortest route is a MinGW/MSYS2 `g++`. Native MSVC support requires a separate `cl` command construction with equivalent optimization, C++17, preprocessor-definition, and output options. |
| `src/graph_drawings/config.py`: the default `SCRATCH_ROOT`; `src/graph_drawings/cli.py`: the default for `--scratch-dir` | If `TMPDIR` is unset, scratch files fall back to `/tmp`. | `/tmp` normally exists, but using `tempfile.gettempdir()` would express the portable intent. | Replace the `/tmp` fallback with `tempfile.gettempdir()` so the default resolves to the user's Windows temporary directory. |

The first three rows affect drawing generation and conversion itself. The GNU
`sort`, GNU `time`, and C++ compiler rows affect only the optional exact
isomorphism reduction through external `flag.cpp`. Consequently, a macOS port
of export-only operation mainly requires spawn-safe process setup and RSS-unit
normalization. A Windows port additionally needs a replacement for the Unix
`resource` module.

## Installation

From a checkout, create an environment and install the package:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

Install the test dependency and run the bounded test suite with:

```bash
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest
```

## Run a census

The following command generates the drawings of `K(2,3)` and exports both flag
representations without reducing the flag rows by isomorphism:

```bash
bipartite-drawing-census 2 3 \
  --workers 4 \
  --output-dir results/K2_3
```

Scratch files default to `$TMPDIR/complete-bipartite-drawing-census/<run-name>`.
An explicit scratch location is recommended for larger cases:

```bash
bipartite-drawing-census 3 4 \
  --workers 32 \
  --output-dir /path/to/results/K3_4 \
  --scratch-dir /fast/local/scratch/K3_4
```

To obtain exact crossing-pair and 4-graph isomorphism-class counts, add the
external source file:

```bash
bipartite-drawing-census 2 3 \
  --workers 4 \
  --output-dir results/K2_3 \
  --flag-cpp /path/to/flag.cpp
```

The last command can be run after an export-only command. It resumes from the
saved drawings and flag rows instead of regenerating them. The SHA-256 digest
of `flag.cpp` is recorded in the run manifest.

Each run writes:

- `run_config.json`, describing the graph and enumeration settings;
- `coordinator.sqlite`, containing the resumable work state;
- compact drawing shards and generation diagnostics under `outputs/`;
- crossing-pair and 4-graph rows under `census/export-shards/`;
- `census/run.json`, with commands, timings, hashes, and stage status;
- `census/summary.json` and `census/report.md`, with the resulting counts.

Compare a completed summary with the published reference counts using:

```bash
bipartite-census-check results/K2_3/census/summary.json
```

Without `flag.cpp`, this checks the drawing count and reports the two flag-class
comparisons as skipped. With a completed external reduction, it checks all
three rows. The reference table is packaged in
[`src/graph_drawings/data/census.json`](src/graph_drawings/data/census.json).
The larger cases require substantial compute. In particular, `K(4,4)` is
intended for a multi-core compute server rather than an interactive
workstation.

## Provenance

The implementation was extracted from the enumerator used for the reported
census. Exact source and run provenance are recorded in
[`docs/provenance.md`](docs/provenance.md).

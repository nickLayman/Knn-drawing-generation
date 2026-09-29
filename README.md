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

- Python 3.12 or newer;
- enough memory and storage for the selected graph;
- Linux for the parallel `--workers` mode;
- optionally, a C++17 `g++` command and a separate copy of `flag.cpp` for exact
  flag-isomorphism reduction. Parallel reduction also requires GNU `sort`.

The drawing generator itself uses only the Python standard library.

### Serial and parallel platform support

`--serial` is the portable verification mode. It executes drawing extension,
canonical reduction, drawing-to-flag conversion, and flag-reduction
orchestration one operation at a time in the main Python process. It starts no
coordinator server, opens no coordinator socket, creates no Python worker
process, and does not use `multiprocessing`. If external `flag.cpp` reduction
is requested, the compiler and each reducer binary are still launched as
sequential subprocesses. The scratch-directory default comes from Python's
platform-specific temporary directory. On Windows, CPU timing remains
available but maximum-RSS fields are recorded as zero because the standard
library has no corresponding process-memory interface.

This mode exists for readers who want to check the small cases with the least
machine-specific setup. It is slower and, when `flag.cpp` is requested, its
in-process deduplication sort holds the labeled flag rows in memory. It is
therefore intended for cases such as `K(2,2)`, `K(2,3)`, and `K(3,3)`, rather
than the largest census runs.

The export-only serial path is supported on a current ordinary installation of
macOS, Windows, or Linux with Python 3.12 or newer. Exact flag-isomorphism
reduction remains optional and requires external `flag.cpp` plus a compatible
C++ compiler. The current compiler command is `g++`; on macOS the Xcode command
line tools normally provide a compatible Clang driver under that name. On
Windows, use a `g++` supplied by MinGW-w64 or MSYS2. Native MSVC is not currently
supported because `compile_reducer` in `src/graph_drawings/cli.py` constructs
GNU-style compiler arguments.

`--workers N` is the Linux parallel mode used for the larger cases. Its Linux
restriction comes from these locations:

| Location | Linux-dependent behavior | Change needed for parallel macOS or Windows support |
|---|---|---|
| `src/graph_drawings/local_runner.py`: `run_local`, `_run_coordinator`, and `_run_worker` | Worker processes inherit the configured output and scratch roots through Linux `fork` behavior. | Pass the runtime roots into every child entry point and call `config.configure_runtime` after a `spawn`, or otherwise initialize every child explicitly. |
| `src/graph_drawings/cli.py`: `export_all_shards` and `reduce_variant` | Parallel pools explicitly request `multiprocessing.get_context("fork")`. | Replace this with a tested `spawn` context. The export initializer must populate all worker state, and every task argument must remain pickleable. |
| `src/graph_drawings/cli.py`: `sort_fixed_rows` | Parallel exact reduction invokes GNU `sort` with `--parallel`, `-S`, `-T`, `-o`, and `LC_ALL=C`. | Select an installed GNU `gsort` on macOS or GNU `sort` from MSYS2 on Windows, or implement a bounded-memory Python external merge sort. BSD `sort` and Windows `sort.exe` are not command-line compatible. |

Process-resource measurements are isolated in
`src/graph_drawings/resource_usage.py`: macOS byte-valued RSS measurements are
converted to KiB, while Windows uses the timing-only fallback. GNU
`/usr/bin/time -v` diagnostics are attempted only on Linux and are not required
for correctness.

## Installation

From a checkout, create an environment and install the package:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

On Windows PowerShell, use `.venv\Scripts\python -m pip install -e .` after
creating the environment with `py -3.12 -m venv .venv`.

Install the test dependency and run the bounded test suite with:

```bash
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest
```

## Run a census

For a straightforward single-process check on macOS, Windows, or Linux:

```bash
bipartite-drawing-census 2 3 \
  --serial \
  --output-dir results/K2_3
```

This writes the six strong drawings of `K(2,3)` and both unreduced flag
representations without starting a coordinator or worker process.

For parallel Linux execution, replace `--serial` with a worker count:

```bash
bipartite-drawing-census 2 3 \
  --workers 4 \
  --output-dir results/K2_3
```

Scratch files default beneath the platform temporary directory (`$TMPDIR` when
that variable is set).
An explicit scratch location is recommended for larger cases:

```bash
bipartite-drawing-census 3 4 \
  --workers 32 \
  --output-dir /path/to/results/K3_4 \
  --scratch-dir /fast/local/scratch/K3_4
```

To obtain exact crossing-pair and 4-graph isomorphism-class counts, add the
external source file. It can be combined with either execution mode:

```bash
bipartite-drawing-census 2 3 \
  --serial \
  --output-dir results/K2_3 \
  --flag-cpp /path/to/flag.cpp
```

The last command can be run after an export-only command. It resumes from the
saved drawings and flag rows instead of regenerating them. The SHA-256 digest
of `flag.cpp` is recorded in the run manifest.

Each run writes:

- `run_config.json`, describing the graph and enumeration settings;
- `coordinator.sqlite`, containing the resumable work state (serial mode keeps
  the file format but does not start a coordinator service);
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

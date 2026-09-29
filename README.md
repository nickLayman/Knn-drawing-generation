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

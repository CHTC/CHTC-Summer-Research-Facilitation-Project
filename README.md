# HTCondor Cluster Analytics Suite

A command-line toolkit for diagnosing and profiling HTCondor job clusters. Given a cluster ID, the suite fetches job data from the HTCondor schedd and produces reports on job status, runtime distribution, resource utilisation, and held-job analysis.

---

## File Overview

| File | Role |
|---|---|
| `main.py` | Unified CLI entry point for all analysis subcommands |
| `fetch_cluster_data.py` | Fetches cluster data from HTCondor schedd and saves to CSV |
| `query.py` | Alternative data fetch via Elasticsearch (for OSPool/CHTC environments) |
| `analytics.py` | Resource utilisation report (CPU, memory, disk efficiency and recommendations) |
| `dashboard.py` | ASCII bar chart of job statuses |
| `histogram.py` | Runtime distribution histogram and scatter plot |
| `hold_bucket.py` | Held-job classifier and bucketer |
| `summarize.py` | Aggregated cluster health report (pulls from all other tools) |
| `utils.py` | Shared utilities (`safe_float`, `load_csv_for_cluster`, time formatters) |

---

## Quickstart

```bash
# Step 1 — fetch and cache job data (only needed once per cluster)
python fetch_cluster_data.py 12345

# Step 2 — run any analysis
python main.py summary   12345   # aggregated health report (good starting point)
python main.py resources 12345   # resource utilisation deep-dive
python main.py runtimes  12345   # runtime distribution
python main.py status    12345   # job status bar chart
python main.py held      12345   # held job analysis
```

Or use the Makefile:

```bash
make fetch   CLUSTER=12345
make summary CLUSTER=12345
make quick   CLUSTER=12345   # fetch + summary in one step
make demo    CLUSTER=12345   # all tools in sequence with prompts
```

> **Note:** If you run a subcommand and no cached CSV exists for that cluster, the data will be fetched automatically before the analysis runs. `fetch_cluster_data.py` only needs to be run explicitly if you want to pre-fetch, refresh stale data, or save to a custom output directory.

---

## Subcommands

### `summary` — Cluster Health Report

Aggregates data from all other tools into a single colour-coded status table with recommended next steps. Good first stop for any cluster.

```bash
python main.py summary 12345
make summary CLUSTER=12345
```

Checks: memory/disk/CPU efficiency, held job rate, fast job rate, runtime consistency.

---

### `resources` — Resource Utilisation Report

Analyses CPU, memory, and disk request vs. actual usage across all jobs.

```bash
python main.py resources 12345
make resources CLUSTER=12345
```

Produces:
- Requested resource breakdown (counts per request level)
- Five-number summary (min, Q1, median, Q3, max, stddev) of actual usage
- Overall utilisation bar chart
- Usage distribution histograms
- Optimisation recommendations based on P95 usage patterns, including estimated GiB-hours savings broken down per job

**Understanding GiB-hours savings:** This unit captures both how much memory is wasted *and* how long jobs run. A saving of 1,200 GiB-hours shown as `(≈ 30.0 GiB/job × 40 jobs × 1.0 hr avg runtime)` means each job wastes ~30 GiB, across 40 jobs, each running ~1 hour on average. You can sanity-check each factor independently against your expectations.

---

### `runtimes` — Runtime Distribution

Plots job runtimes from cached CSV data.

```bash
python main.py runtimes 12345
python main.py runtimes 12345 --percentiles 20
python main.py runtimes 12345 --print-list
make runtimes CLUSTER=12345
```

Produces:
- Scatter plot of job index vs. runtime (detects trends — e.g. later jobs running slower)
- Percentile-binned ASCII histogram with red highlighting for bins whose median runtime is under 10 minutes (a signal that jobs may benefit from bundling)

| Flag | Default | Description |
|---|---|---|
| `--percentiles N` | `10` | Number of percentile bins in the histogram |
| `--print-list` | off | Print job IDs (`ClusterId.ProcId`) in fast-job bins |

---

### `status` — Job Status Bar Chart

Queries the HTCondor schedd directly (history + queue) and renders a live bar chart of job statuses.

```bash
python main.py status 12345
make status CLUSTER=12345
```

Statuses shown: Idle, Running, Removing, Completed, Held, Transferring Output, Suspended.

> Unlike other subcommands, `status` does not use the cached CSV — it always queries the schedd live.

---

### `held` — Held Job Analysis

Queries the schedd for all held jobs in the cluster and groups them by hold reason code, using fuzzy string matching to bucket jobs with similar error messages.

```bash
python main.py held 12345
python main.py held 12345 --min-count 5 --sort-by time
python main.py held 12345 --top 5 --code 34
python main.py held 12345 --show-job-ids --export-jobs held.csv
make held CLUSTER=12345
```

**Filtering options:**

| Flag | Default | Description |
|---|---|---|
| `--min-count N` | `1` | Only show buckets with at least N jobs |
| `--top N` | off | Show only the top N buckets |
| `--code CODE` | off | Filter to a specific `HoldReasonCode` |

**Sorting options:**

| Flag | Choices | Default |
|---|---|---|
| `--sort-by` | `count`, `code`, `percent`, `time` | `count` |

**Bucketing options:**

| Flag | Default | Description |
|---|---|---|
| `--threshold RATIO` | `0.7` | Similarity threshold (0.0–1.0) for grouping similar error messages. Higher = stricter. |

**Output options:**

| Flag | Description |
|---|---|
| `--show-job-ids` | Display ProcIds in the output table |
| `--export-jobs FILENAME` | Export held job IDs to a CSV (`ClusterId.ProcId`, code, label) |

---

## Data Fetch

### `fetch_cluster_data.py` — HTCondor Schedd

Fetches all job records for a cluster from the HTCondor schedd (history + queue) and writes them to `cluster_data/cluster_<ID>_jobs.csv`.

```bash
python fetch_cluster_data.py 12345
python fetch_cluster_data.py 12345 /path/to/output/dir
make fetch CLUSTER=12345
```

| Argument | Required | Description |
|---|---|---|
| `CLUSTER_ID` | Yes | HTCondor cluster ID (must be an integer) |
| `OUTPUT_DIR` | No | Directory for the CSV (default: `cluster_data/`) |

The script validates that the cluster exists before fetching. Passing a non-integer cluster ID raises an error immediately rather than scanning the full job history.

---

### `query.py` — Elasticsearch (OSPool / CHTC)

An alternative fetch for environments where job history is stored in Elasticsearch. Uses the Scroll API to handle large result sets.

```bash
python query.py 12345
python query.py 12345 --user alice
python query.py 12345 --output-dir /path/to/dir
make fetch-es CLUSTER=12345
```

| Argument | Required | Description |
|---|---|---|
| `CLUSTER_ID` | Yes | HTCondor cluster ID (must be an integer) |
| `--user USER` | No | Filter jobs to a specific `Owner` field |
| `--output-dir DIR` | No | Output directory (default: `cluster_data/`) |

**Authentication:** Fill in `ES_USER` and `ES_PASS` at the top of `query.py` before running. These are left blank in the repository.

**Output filename:** Always written to `cluster_data/cluster_<ID>_jobs.csv` regardless of `--user`, so the rest of the suite can find it automatically.

---

## Makefile

The Makefile provides convenient shortcuts for all common workflows.

```bash
make fetch    CLUSTER=12345   # fetch from HTCondor schedd
make fetch-es CLUSTER=12345   # fetch from Elasticsearch
make summary  CLUSTER=12345   # health report
make resources CLUSTER=12345  # resource utilisation
make runtimes CLUSTER=12345   # runtime histogram
make status   CLUSTER=12345   # job status chart
make held     CLUSTER=12345   # held job analysis
make quick    CLUSTER=12345   # fetch + summary
make all      CLUSTER=12345   # all analysis tools (data must already be fetched)
make demo     CLUSTER=12345   # all tools in sequence with prompts
make compare  CLUSTER=12345 CLUSTER2=67890  # side-by-side summary of two clusters
make list                     # show locally cached cluster IDs
make clean                    # delete cached CSV files
make check                    # verify dependencies are installed
make install                  # pip install -r requirements.txt
```

---

## Data & Caching

All analysis subcommands (except `status` and `held`) read from a cached CSV at:

```
cluster_data/cluster_<ID>_jobs.csv
```

This file is created by `fetch_cluster_data.py` or `query.py`, or automatically by `main.py` on first use. To refresh stale data, re-run the fetch script — it overwrites the existing file.

---

## Dependencies

- Python 3.8+
- `htcondor2` (HTCondor Python bindings — typically provided by the system HTCondor installation rather than pip)
- `numpy`
- `tabulate`
- `elasticsearch` (only required for `query.py` / `make fetch-es`)
import sys
import os
import csv
import argparse
import textwrap
import numpy as np
from datetime import datetime, timedelta


"""
This program takes data from the cluster_data folder and gives an ASCII histogram
of the runtimes for a cluster. The runtimes are grouped by percentile range.
"""


def parse_args():
    parser = argparse.ArgumentParser(
        prog="condor_histogram",
        description=textwrap.dedent(
            """
            HTCondor Cluster Runtime Histogram

            Reads job execution data from CSV files and produces:
              • A scatter plot of job index vs. runtime (detects trends)
              • A percentile-binned ASCII histogram of job runtimes
              • Flags jobs with median runtime < 10 minutes (in red)

            Data files must be located in: cluster_data/cluster_{id}_jobs.csv

            Example usage:
              python histogram.py 12345
              python histogram.py 12345 --print-list
              python histogram.py 12345 --percentiles 20
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "-cluster_id",
        "--cluster_id",
        required=True,
        metavar="ID",
        help="HTCondor cluster ID to analyze (required).",
    )

    parser.add_argument(
        "--print-list",
        action="store_true",
        default=False,
        help="Print the list of job IDs with median runtime < 10 minutes.\n"
             "Useful for identifying specific short-running jobs. (default: off)",
    )

    parser.add_argument(
        "--percentiles",
        type=int,
        default=10,
        metavar="N",
        help="Number of percentile bins for the histogram. (default: 10)",
    )

    return parser.parse_args()


def format_seconds_human(seconds):
    seconds = int(seconds)
    if seconds == 0:
        return "0s"
    parts = []
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def format_epoch_human_relative(epoch_seconds):
    try:
        event_time = datetime.fromtimestamp(int(epoch_seconds))
        now = datetime.now()
        delta = now - event_time

        if delta < timedelta(minutes=1):
            return "just now"
        elif delta < timedelta(hours=1):
            minutes = int(delta.total_seconds() // 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif delta < timedelta(days=1):
            hours = int(delta.total_seconds() // 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif delta < timedelta(days=7):
            days = delta.days
            return f"{days} day{'s' if days != 1 else ''} ago"
        elif delta < timedelta(days=30):
            weeks = delta.days // 7
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        else:
            return event_time.strftime("%Y-%m-%d")
    except Exception:
        return "N/A"


def safe_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def scatter_plot_job_index_vs_runtime(cluster_id, jobs, height=12, width=60):
    job_indices = []
    runtimes = []

    for idx, job in enumerate(jobs):
        runtime = safe_float(job.get("RemoteWallClockTime"))
        if runtime is not None:
            job_indices.append(idx)
            runtimes.append(runtime)

    if not runtimes:
        print("[WARN] No valid runtime data for scatter plot.")
        return

    job_indices = np.array(job_indices)
    runtimes = np.array(runtimes)

    max_index = len(jobs) - 1
    p95_runtime = np.percentile(runtimes, 95)
    max_runtime = p95_runtime
    outliers = np.sum(runtimes > p95_runtime)

    x_positions = (
        (job_indices / max_index * (width - 1)).astype(int)
        if max_index > 0
        else np.zeros(len(job_indices), dtype=int)
    )
    capped_runtimes = np.minimum(runtimes, max_runtime)
    y_positions = (
        (max_runtime - capped_runtimes) / max_runtime * (height - 1)
    ).astype(int)

    plot = [[" " for _ in range(width)] for _ in range(height)]
    density = [[0 for _ in range(width)] for _ in range(height)]

    for x, y in zip(x_positions, y_positions):
        if 0 <= y < height and 0 <= x < width:
            density[y][x] += 1

    for y in range(height):
        for x in range(width):
            if density[y][x] > 0:
                if density[y][x] == 1:
                    plot[y][x] = "·"
                elif density[y][x] <= 3:
                    plot[y][x] = "•"
                else:
                    plot[y][x] = "█"

    correlation = np.corrcoef(job_indices, runtimes)[0, 1]

    third = len(runtimes) // 3
    if third > 0:
        first_third_median = np.median(runtimes[:third])
        last_third_median = np.median(runtimes[-third:])
    else:
        first_third_median = last_third_median = np.median(runtimes)

    median_runtime = np.median(runtimes)

    print(f"\n{'Job Index vs Runtime Scatter Plot':^70}")
    print("=" * 70)
    print(
        f"Jobs: {len(runtimes)}  |  Median: {format_seconds_human(median_runtime)}"
        f"  |  Correlation: {correlation:.3f}"
    )

    if correlation > 0.4:
        print("Trend: Later jobs run LONGER ⚠️")
    elif correlation < -0.4:
        print("Trend: Later jobs run FASTER ✓")
    else:
        print("Trend: Consistent runtime across jobs ✓")
    print()

    for i in range(height):
        runtime_val = max_runtime - (i / (height - 1)) * max_runtime

        if i == 0:
            label = format_seconds_human(runtime_val)
            print(f"{label:>9} |", end="")
        elif i == height - 1:
            print(f"{'0s':>9} |", end="")
        elif i == height // 2:
            label = format_seconds_human(runtime_val)
            print(f"{label:>9} |", end="")
        else:
            print(f"{'':10} |", end="")

        print("".join(plot[i]))

    print(f"{'':10} +{'-' * width}")
    print(
        f"{'':12}0{' ' * (width // 2 - 5)}{max_index // 2}"
        f"{' ' * (width // 2 - 5)}{max_index}"
    )
    print(f"{'':12}Job Index")

    print(f"\nSymbols: · = 1 job   • = 2-3 jobs   █ = 4+ jobs")
    if outliers > 0:
        print(
            f"Note: {outliers} job(s) with runtime > "
            f"{format_seconds_human(p95_runtime)} (95th percentile) not shown"
        )
    print()


def histogram(cluster_id, jobs, percentiles=10, max_width=20, show_fast_jobs=False):
    if not jobs:
        print("[WARN] No valid data to plot.")
        return

    runtimes = []
    cluster_ids = []
    proc_ids = []
    submit_times = []
    completion_times = []

    for job in jobs:
        runtime = safe_float(job.get("RemoteWallClockTime"))
        if runtime is not None:
            runtimes.append(runtime)
            cluster_ids.append(job.get("ClusterId", ""))
            proc_ids.append(job.get("ProcId", ""))

            qdate = safe_float(job.get("QDate"))
            if qdate is not None:
                submit_times.append(qdate)

            comp_date = safe_float(job.get("CompletionDate"))
            if comp_date is not None:
                completion_times.append(comp_date)

    if not runtimes:
        print("[WARN] No valid runtime data to plot.")
        return

    runtimes = np.array(runtimes)
    cluster_ids = np.array(cluster_ids)
    proc_ids = np.array(proc_ids)

    percentiles_list = np.linspace(0, 100, percentiles + 1)
    bin_edges = np.percentile(runtimes, percentiles_list)
    counts, _ = np.histogram(runtimes, bins=bin_edges)
    max_count = counts.max()

    print(f"\n{'Histogram of Job Runtimes by Percentiles':^80}")
    print("=" * 80)
    print(f"ClusterId: {cluster_id}\n")

    if submit_times:
        print(f"First Submitted : {format_epoch_human_relative(min(submit_times))}")
    else:
        print("First Submitted : N/A")

    if completion_times:
        print(f"Last Completed  : {format_epoch_human_relative(max(completion_times))}")
    else:
        print("Last Completed  : N/A")

    print("")

    pct_width = 11
    label_width = 30
    count_width = 7

    RED = "\033[91m"
    RESET = "\033[0m"

    header = (
        f"{'Percentile':<{pct_width}}"
        f"{'Time Range':<{label_width}}"
        f"| {'Histogram':<{max_width}}"
        f" {'# Jobs':>{count_width}}"
    )
    print(header)
    print("-" * len(header))

    jobs_under_10_min_median = 0
    fast_job_ids = []

    for i in range(len(counts)):
        left = bin_edges[i]
        right = bin_edges[i + 1]

        in_bin_mask = (
            (runtimes >= left) & (runtimes <= right)
            if i == len(counts) - 1
            else (runtimes >= left) & (runtimes < right)
        )

        in_bin_times = runtimes[in_bin_mask]
        in_bin_clusters = cluster_ids[in_bin_mask]
        in_bin_procs = proc_ids[in_bin_mask]

        median_time = np.median(in_bin_times) if len(in_bin_times) > 0 else 0
        is_red = median_time < 600

        color = RED if is_red else ""
        if is_red:
            jobs_under_10_min_median += len(in_bin_times)
            fast_job_ids.extend(
                [f"{cid}.{pid}" for cid, pid in zip(in_bin_clusters, in_bin_procs)]
            )

        left_label = format_seconds_human(left)
        right_label = format_seconds_human(right)
        time_range = f"{left_label:>10} - {right_label:>10}".rjust(label_width)

        pct_start = int(percentiles_list[i])
        pct_end = int(percentiles_list[i + 1])
        pct_range = f"{pct_start:02}–{pct_end:02}%".ljust(pct_width)

        bar = "█" * int((counts[i] / max_count) * max_width)
        colored_bar = f"{color}{bar:<{max_width}}{RESET}"

        print(f"{pct_range}{time_range} | {colored_bar} {counts[i]:>{count_width}}")

    print(f"\n{RED}Note:{RESET} Bars in red represent bins with median runtime < 10 minutes.")
    print(f"{RED}Info:{RESET} Total number of jobs in such bins: {jobs_under_10_min_median}")

    if show_fast_jobs and fast_job_ids:
        print(f"\nList of Job IDs with median runtime < 10 minutes:")
        print(", ".join(fast_job_ids))


def load_data_for_cluster(cluster_id):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        print(
            "Cluster Data not found, please make sure you have the correct "
            ".csv, filepath and the correct cluster id"
        )
        sys.exit(1)

    with open(filepath, newline="", encoding="utf-8") as f:
        jobs = list(csv.DictReader(f))

    return jobs


def get_histogram_data(cluster_id):
    """
    Return runtime analysis data as a dictionary for use by cluster_health.py.
    Does not print anything, just returns computed metrics.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        return None

    with open(filepath, newline="", encoding="utf-8") as f:
        jobs = list(csv.DictReader(f))

    if not jobs:
        return None

    runtimes = []
    submit_times = []
    completion_times = []

    for job in jobs:
        runtime = safe_float(job.get("RemoteWallClockTime"))
        if runtime is not None and runtime > 0:
            runtimes.append(runtime)

        qdate = safe_float(job.get("QDate"))
        if qdate is not None:
            submit_times.append(qdate)

        comp_date = safe_float(job.get("CompletionDate"))
        if comp_date is not None:
            completion_times.append(comp_date)

    if not runtimes:
        return None

    runtimes = np.array(runtimes)

    mean_runtime = np.mean(runtimes)
    median_runtime = np.median(runtimes)
    std_runtime = np.std(runtimes)
    cv = (std_runtime / mean_runtime) if mean_runtime > 0 else 0
    fast_jobs = np.sum(runtimes < 600)
    fast_jobs_pct = (fast_jobs / len(runtimes)) * 100
    p95 = np.percentile(runtimes, 95)
    long_jobs = np.sum(runtimes > p95)
    job_indices = np.arange(len(runtimes))
    correlation = np.corrcoef(job_indices, runtimes)[0, 1]

    return {
        "total_runtime_jobs": len(runtimes),
        "mean_runtime": mean_runtime,
        "median_runtime": median_runtime,
        "std_runtime": std_runtime,
        "cv": cv,
        "fast_jobs": fast_jobs,
        "fast_jobs_pct": fast_jobs_pct,
        "long_jobs": long_jobs,
        "p95_runtime": p95,
        "min_runtime": np.min(runtimes),
        "max_runtime": np.max(runtimes),
        "correlation": correlation,
        "first_submitted": min(submit_times) if submit_times else None,
        "last_completed": max(completion_times) if completion_times else None,
    }


def run(args):
    """Entry point used by both standalone and main.py subcommand."""
    jobs = load_data_for_cluster(args.cluster_id)
    scatter_plot_job_index_vs_runtime(args.cluster_id, jobs, height=15, width=60)
    histogram(
        args.cluster_id,
        jobs,
        percentiles=args.percentiles,
        max_width=20,
        show_fast_jobs=args.print_list,
    )


if __name__ == "__main__":
    args = parse_args()
    run(args)
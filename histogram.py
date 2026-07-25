import numpy as np
import pandas as pd
from utils import load_csv_for_cluster, format_seconds_human, format_epoch_human_relative

"""
This program takes data from the cluster_data folder and gives an ASCII histogram
and ASCII CDF of the runtimes for a cluster.
"""


# ── Data loading ───────────────────────────────────────────────────────────────

def load_data_for_cluster(cluster_id):
    """Load cluster CSV and return a cleaned DataFrame."""
    jobs = load_csv_for_cluster(cluster_id)
    df = pd.DataFrame(jobs)

    for col in ["RemoteWallClockTime", "QDate", "CompletionDate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _get_positive_runtimes(df):
    """Return positive runtime Series, or None if missing/empty."""
    if "RemoteWallClockTime" not in df.columns:
        print("[WARN] Missing RemoteWallClockTime column.")
        return None

    rt = df["RemoteWallClockTime"].dropna()
    rt = rt[rt > 0]

    if rt.empty:
        print("[WARN] No valid runtime data found.")
        return None

    return rt


# ── Cumulative Distribution ────────────────────────────────────────────────────

def cumulative_distribution(cluster_id, df, height=20, width=60):
    """
    Print an ASCII CDF line plot of job runtimes.

    X-axis: runtime over full observed range.
    Y-axis: cumulative % of jobs.
    Reads as: "X% of jobs completed within Y time."
    """
    rt = _get_positive_runtimes(df)
    if rt is None:
        return

    runtimes = np.sort(rt.values)
    n = len(runtimes)

    marker_pcts = [25, 50, 75, 90, 95]
    marker_times = {p: float(np.percentile(runtimes, p)) for p in marker_pcts}

    x_max = float(runtimes[-1])
    plot = [[" " for _ in range(width)] for _ in range(height)]

    prev_y = height - 1
    for x in range(width):
        rt_at_x = (x / (width - 1)) * x_max if width > 1 else x_max
        cum_pct = float(np.searchsorted(runtimes, rt_at_x, side="right")) / n
        y = int((1.0 - cum_pct) * (height - 1))
        y = max(0, min(height - 1, y))

        y_lo, y_hi = min(y, prev_y), max(y, prev_y)
        for fy in range(y_lo, y_hi + 1):
            plot[fy][x] = "·"
        plot[y][x] = "•"
        prev_y = y

    # Percentile guideline rows.
    # Multiple percentiles may map to the same row, so store a list.
    marker_rows = {}
    for p, t in marker_times.items():
        row = int((1.0 - p / 100.0) * (height - 1))
        row = max(0, min(height - 1, row))
        marker_rows.setdefault(row, []).append((p, t))

        for x in range(width):
            if plot[row][x] == " ":
                plot[row][x] = "╌"

    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"

    label_w = 5

    print(f"  {'Cumulative Distribution':^{width}}")
    print(f"{'':>{label_w}}  {'-' * width}")

    for i, row in enumerate(plot):
        pct_val = round(100 * (1.0 - i / (height - 1))) if height > 1 else 100
        label = f"{pct_val}%" if pct_val in (0, 25, 50, 75, 100) else ""

        annotation = ""
        if i in marker_rows:
            parts = [
                f"{format_seconds_human(t)} (P{p})"
                for p, t in sorted(marker_rows[i], key=lambda x: x[0])
            ]
            annotation = "  " + " | ".join(parts)

        coloured = []
        for cell in row:
            if cell in ("•", "·"):
                coloured.append(f"{CYAN}{cell}{RESET}")
            elif cell == "╌":
                coloured.append(f"{YELLOW}{cell}{RESET}")
            else:
                coloured.append(cell)

        print(f"{label:>{label_w}} |{''.join(coloured)}{annotation}")

    print(f"{'':>{label_w}} +{'-' * width}")

    col = max(1, width // 4)
    ticks = (
        f"{'0s':<{col}}"
        f"{format_seconds_human(x_max * 0.25):<{col}}"
        f"{format_seconds_human(x_max * 0.50):<{col}}"
        f"{format_seconds_human(x_max * 0.75):<{max(1, col - 1)}}"
        f"{format_seconds_human(x_max):>{col}}"
    )
    print(f"{'':>{label_w + 1}} {ticks}")
    print(f"{'':>{label_w + 1}} {'Runtime':^{width}}")

    print(
        f"\n  Key:  {CYAN}•{RESET} curve point   "
        f"{CYAN}·{RESET} connector   "
        f"{YELLOW}╌{RESET} percentile guideline"
    )
    print()


# ── Histogram ──────────────────────────────────────────────────────────────────

def histogram(cluster_id, df, percentiles=10, max_width=20, show_fast_jobs=False):
    """
    Print histogram where bins are percentile ranges.
    Bars are red if the bin median runtime is < 10 minutes.
    Optionally print exact job IDs whose runtime is < 10 minutes.
    """
    rt = _get_positive_runtimes(df)
    if rt is None:
        return

    runtimes = rt.values
    percentiles_list = np.linspace(0, 100, percentiles + 1)
    raw_edges = np.percentile(runtimes, percentiles_list)
    bin_edges = np.unique(raw_edges)

    if len(bin_edges) < 2:
        print("[WARN] Not enough unique runtime values to build histogram.")
        return

    counts, _ = np.histogram(runtimes, bins=bin_edges)
    max_count = counts.max() if len(counts) > 0 else 0

    print(f"\n  {'Histogram by Percentile':^80}")
    print(f"  {'-' * 80}")
    print()

    pct_width = 15
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

    jobs_in_red_bins = 0

    for i in range(len(counts)):
        left = bin_edges[i]
        right = bin_edges[i + 1]

        mask = (
            (rt >= left) & (rt <= right)
            if i == len(counts) - 1
            else (rt >= left) & (rt < right)
        )

        in_bin = rt[mask]
        median_time = in_bin.median() if not in_bin.empty else 0
        is_red = median_time < 600

        if is_red:
            jobs_in_red_bins += len(in_bin)

        color = RED if is_red else ""
        time_range = (
            f"{format_seconds_human(left):>10} - {format_seconds_human(right):>10}"
        ).rjust(label_width)

        # Because duplicate percentile edges may have been collapsed by np.unique,
        # compute an approximate percentile label from the actual edge positions.
        left_pct = int(round(100 * i / len(counts)))
        right_pct = int(round(100 * (i + 1) / len(counts)))
        pct_range = f"{left_pct:02}–{right_pct:02}%".ljust(pct_width)

        bar_len = int((counts[i] / max_count) * max_width) if max_count > 0 else 0
        bar = "█" * bar_len

        print(
            f"{pct_range}{time_range} | "
            f"{color}{bar:<{max_width}}{RESET} {counts[i]:>{count_width}}"
        )

    print(f"\n{RED}Note:{RESET} Bars in red represent bins with median runtime < 10 minutes.")
    print(f"{RED}Info:{RESET} Total number of jobs in such bins: {jobs_in_red_bins}")

    if show_fast_jobs:
        if "ClusterId" in df.columns and "ProcId" in df.columns:
            fast_mask = df["RemoteWallClockTime"].fillna(-1) < 600
            fast_jobs = df.loc[fast_mask, ["ClusterId", "ProcId"]].dropna()

            fast_job_ids = [
                f"{int(r)}.{int(p)}" if pd.notna(r) and pd.notna(p) else f"{r}.{p}"
                for r, p in zip(fast_jobs["ClusterId"], fast_jobs["ProcId"])
            ]

            if fast_job_ids:
                print(f"\nJob IDs with runtime < 10 minutes:")
                print(", ".join(fast_job_ids))
            else:
                print(f"\nNo jobs with runtime < 10 minutes.")
        else:
            print("\n[WARN] Cannot print fast job IDs: missing ClusterId or ProcId column.")


# ── Data-only interface for summarize.py ──────────────────────────────────────

def get_histogram_data(cluster_id):
    """
    Return runtime analysis metrics as a dict for use by summarize.py.
    Does not print anything.
    """
    jobs = load_csv_for_cluster(cluster_id, exit_on_missing=False)
    if not jobs:
        return None

    df = pd.DataFrame(jobs)
    for col in ["RemoteWallClockTime", "QDate", "CompletionDate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "RemoteWallClockTime" not in df.columns:
        return None

    rt = df["RemoteWallClockTime"].dropna()
    rt = rt[rt > 0]
    if rt.empty:
        return None

    runtimes = rt.values
    p95 = np.percentile(runtimes, 95)

    qdate_series = df["QDate"].dropna() if "QDate" in df.columns else pd.Series(dtype=float)
    completion_series = (
        df["CompletionDate"].dropna()
        if "CompletionDate" in df.columns else pd.Series(dtype=float)
    )

    return {
        "total_runtime_jobs": len(runtimes),
        "mean_runtime": rt.mean(),
        "median_runtime": rt.median(),
        "std_runtime": rt.std(),
        "cv": rt.std() / rt.mean() if rt.mean() > 0 else 0,
        "fast_jobs": int((runtimes < 600).sum()),
        "fast_jobs_pct": float((runtimes < 600).mean() * 100),
        "long_jobs": int((runtimes > p95).sum()),
        "p95_runtime": p95,
        "min_runtime": rt.min(),
        "max_runtime": rt.max(),
        "first_submitted": qdate_series.min() if not qdate_series.empty else None,
        "last_completed": completion_series.max() if not completion_series.empty else None,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

def run(args):
    """Entry point called by main.py."""
    df = load_data_for_cluster(args.cluster_id)

    rt = _get_positive_runtimes(df)
    n = len(rt) if rt is not None else 0

    submit_times = df["QDate"].dropna() if "QDate" in df.columns else pd.Series(dtype=float)
    completion_times = (
        df["CompletionDate"].dropna()
        if "CompletionDate" in df.columns else pd.Series(dtype=float)
    )

    first_sub = (
        format_epoch_human_relative(submit_times.min())
        if not submit_times.empty else "N/A"
    )
    last_comp = (
        format_epoch_human_relative(completion_times.max())
        if not completion_times.empty else "N/A"
    )

    BOLD = "\033[1m"
    RESET = "\033[0m"

    print(f"\n{BOLD}{'Runtime Analysis':^80}{RESET}")
    print("=" * 80)
    print(
        f"  Cluster : {args.cluster_id}   |   Jobs: {n}   |   "
        f"Submitted: {first_sub}   |   Completed: {last_comp}"
    )
    print("=" * 80)

    show = getattr(args, "show", "both")
    if show in ("cdf", "both"):
        cumulative_distribution(args.cluster_id, df, height=15, width=60)
    if show in ("histogram", "both"):
        histogram(
            args.cluster_id,
            df,
            percentiles=args.percentiles,
            max_width=20,
            show_fast_jobs=args.print_list,
        )
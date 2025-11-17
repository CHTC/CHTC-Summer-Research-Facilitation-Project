import sys
import os
import pandas as pd
import numpy as np
import subprocess
from datetime import datetime, timedelta

"""
This program takes data from the cluster_data folder and gives an ASCII histogram
of the runtimes for a cluster. The runtimes are grouped by percentile range of the runtimes

"""



# function to format seconds into human readable format
def format_seconds_human(seconds):
    seconds = int(seconds)
    if seconds == 0:
        return "0s"
    parts = []
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if seconds: parts.append(f"{seconds}s")
    return ' '.join(parts)


# function to format seconds into human relative format
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
    except:
        return "N/A"


def histogram(cluster_id, df, percentiles=10, max_width=20, show_fast_jobs=False):

    # Check if DataFrame is empty or missing runtime data
    if df.empty or "RemoteWallClockTime" not in df.columns:
        print("[WARN] No valid data to plot.")
        return
    
    # Extract job runtime data and identifiers as numpy arrays for faster computation
    runtimes = df["RemoteWallClockTime"].astype(float).to_numpy()
    cluster_ids = df["ClusterId"].astype(str).to_numpy()
    proc_ids = df["ProcId"].astype(str).to_numpy()

    # Create evenly-spaced percentile boundaries (e.g., 0%, 10%, 20%, ..., 100%)
    # percentiles=10 creates 11 boundaries defining 10 bins
    percentiles_list = np.linspace(0, 100, percentiles + 1)
    
    # Calculate the actual runtime values at each percentile boundary
    bin_edges = np.percentile(runtimes, percentiles_list)
    
    # Count how many jobs fall into each percentile bin
    counts, _ = np.histogram(runtimes, bins=bin_edges)
    
    # Find the bin with the most jobs (used to scale the bar chart)
    max_count = counts.max()

    print("\nHistogram of Job Runtimes by Percentiles:\n")
    print(f"ClusterId: {cluster_id}\n")

    # Display cluster submission and completion time range
    if "QDate" in df.columns and "CompletionDate" in df.columns:
        submit_times = df["QDate"].dropna().astype(float)  # Unix epoch timestamps
        completion_times = df["CompletionDate"].dropna().astype(float)

        if not submit_times.empty:
            # Find when the first job in the cluster was submitted
            cluster_submit_time = format_epoch_human_relative(submit_times.min())
            print(f"First Submitted : {cluster_submit_time}")
        else:
            print("First Submitted : N/A")

        if not completion_times.empty:
            # Find when the last job in the cluster completed
            cluster_completion_time = format_epoch_human_relative(completion_times.max())
            print(f"Last Completed  : {cluster_completion_time}")
        else:
            print("Last Completed  : N/A")

        print("") 

    # Column widths for formatted output
    pct_width = 11      # Width for percentile range 
    label_width = 30    # Width for time range labels
    count_width = 7     # Width for job count numbers
    
    # ANSI color codes for highlighting fast jobs
    RED = "\033[91m"
    RESET = "\033[0m"

    # table header
    header = (
        f"{'Percentile':<{pct_width}}"
        f"{'Time Range':<{label_width}}"
        f"| {'Histogram':<{max_width}}"
        f" {'# Jobs':>{count_width}}"
    )
    print(header)
    print("-" * len(header))

    # Track jobs that complete very quickly (potential efficiency issues)
    jobs_under_10_min_median = 0   # Count of jobs in bins with median < 10 minutes
    fast_job_ids = []              # List of specific job IDs

    # Iterate through each percentile bin
    for i in range(len(counts)):
        # Get the runtime boundaries for this bin
        left = bin_edges[i]      
        right = bin_edges[i + 1]  
        
        # Last bin includes right edge (<=), others exclude it (<)
        in_bin_mask = (runtimes >= left) & (runtimes <= right) if i == len(counts) - 1 else (runtimes >= left) & (runtimes < right)
        
        # Extract data for jobs in this bin
        in_bin_times = runtimes[in_bin_mask]
        in_bin_clusters = cluster_ids[in_bin_mask]
        in_bin_procs = proc_ids[in_bin_mask]

        # Calculate median runtime for jobs in this bin
        # Median is more robust than mean for skewed distributions
        median_time = np.median(in_bin_times) if len(in_bin_times) > 0 else 0

        # Flag bins where median runtime < 10 minutes (600 seconds)
        # These may indicate jobs that are too short to efficiently use cluster resources
        is_red = median_time < 600

        # Apply red color highlighting to fast job bins
        color = RED if is_red else ""
        if is_red:
            # Track fast jobs for reporting
            jobs_under_10_min_median += len(in_bin_times)
            fast_job_ids.extend([f"{cid}.{pid}" for cid, pid in zip(in_bin_clusters, in_bin_procs)])

        # Format time range labels (e.g., "5.2 min - 12.3 min")
        left_label = format_seconds_human(left)
        right_label = format_seconds_human(right)
        time_range = f"{left_label:>10} - {right_label:>10}".rjust(label_width)

        # Format percentile range (e.g., "00–10%")
        pct_start = int(percentiles_list[i])
        pct_end = int(percentiles_list[i + 1])
        pct_range = f"{pct_start:02}–{pct_end:02}%".ljust(pct_width)

        # Create bar chart: scale bar length proportionally to job count
        # Bar length = (jobs in bin / max jobs in any bin) * max_width
        bar = "█" * int((counts[i] / max_count) * max_width)
        colored_bar = f"{color}{bar:<{max_width}}{RESET}"

        # Print the row for this bin
        print(f"{pct_range}{time_range} | {colored_bar} {counts[i]:>{count_width}}")

    # Summary information about potentially inefficient jobs
    print(f"\n{RED}Note:{RESET} Bars in red represent bins with median runtime < 10 minutes.")
    print(f"{RED}Info:{RESET} Total number of jobs in such bins: {jobs_under_10_min_median}")

    # Optionally show specific job IDs for investigation
    if show_fast_jobs and fast_job_ids:
        print(f"\nList of Job IDs with median runtime < 10 minutes:")
        print(", ".join(fast_job_ids))

# function to get data from the folder or call the query 
def load_data_for_cluster(cluster_id):
    path = f"cluster_data/cluster_{cluster_id}_jobs.csv"
    if not os.path.exists(path):
        print(f"[INFO] CSV for ClusterId {cluster_id} not found. Attempting to run query.py...")
        try:
            subprocess.run([sys.executable, "query.py", cluster_id], check=True)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] query.py failed: {e}")
            return None
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        print(f"[ERROR] Could not load CSV: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python histogram.py <clusterId> [printList]")
        sys.exit(1)

    cluster_id = sys.argv[1]
    print_list_flag = sys.argv[2].lower() in ("true", "yes", "1") if len(sys.argv) > 2 else False

    df = load_data_for_cluster(cluster_id)
    if df is not None:
        histogram(cluster_id, df, percentiles=10, max_width=20, show_fast_jobs=print_list_flag)

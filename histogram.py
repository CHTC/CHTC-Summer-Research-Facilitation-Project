import sys
import os
import csv
import numpy as np
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


def safe_float(value):
    """Safely convert a value to float, returning None if conversion fails"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def scatter_plot_job_index_vs_runtime(cluster_id, jobs, height=12, width=60):
    """
    Scatter plot: Job Index vs Runtime
    Shows runtime trends across job sequence (useful for detecting patterns)
    """
    
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
    
    # Use 95th percentile as max instead of absolute max to handle outliers better
    p95_runtime = np.percentile(runtimes, 95)
    max_runtime = p95_runtime
    
    # Count outliers above 95th percentile
    outliers = np.sum(runtimes > p95_runtime)
    
    # Normalize to plot dimensions
    x_positions = (job_indices / max_index * (width - 1)).astype(int) if max_index > 0 else np.zeros(len(job_indices), dtype=int)
    
    # Cap y positions at max_runtime for better visualization
    capped_runtimes = np.minimum(runtimes, max_runtime)
    y_positions = ((max_runtime - capped_runtimes) / max_runtime * (height - 1)).astype(int)
    
    # Create plot grid - use different symbols for density
    plot = [[' ' for _ in range(width)] for _ in range(height)]
    density = [[0 for _ in range(width)] for _ in range(height)]
    
    # Count points at each position
    for x, y in zip(x_positions, y_positions):
        if 0 <= y < height and 0 <= x < width:
            density[y][x] += 1
    
    # Place symbols based on density
    for y in range(height):
        for x in range(width):
            if density[y][x] > 0:
                if density[y][x] == 1:
                    plot[y][x] = '·'
                elif density[y][x] <= 3:
                    plot[y][x] = '•'
                else:
                    plot[y][x] = '█'
    
    # Calculate correlation to detect trends
    correlation = np.corrcoef(job_indices, runtimes)[0, 1]
    
    # Calculate median runtime for first/last thirds
    third = len(runtimes) // 3
    if third > 0:
        first_third_median = np.median(runtimes[:third])
        last_third_median = np.median(runtimes[-third:])
    else:
        first_third_median = last_third_median = np.median(runtimes)
    
    median_runtime = np.median(runtimes)
    
    print(f"\n{'Job Index vs Runtime Scatter Plot':^70}")
    print("=" * 70)
    print(f"Jobs: {len(runtimes)}  |  Median: {format_seconds_human(median_runtime)}  |  Correlation: {correlation:.3f}")
    
    if correlation > 0.4:
        print("Trend: Later jobs run LONGER ⚠️")
    elif correlation < -0.4:
        print("Trend: Later jobs run FASTER ✓")
    else:
        print("Trend: Consistent runtime across jobs ✓")
    print()
    
    # Print Y-axis (Runtime) and plot
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
            print(f"{'':<10} |", end="")
        
        print(''.join(plot[i]))
    
    # Print X-axis (Job Index)
    print(f"{'':<10} +{'-' * width}")
    print(f"{'':<12}0{' ' * (width//2 - 5)}{max_index // 2}{' ' * (width//2 - 5)}{max_index}")
    print(f"{'':<12}Job Index")
    
    # Print legend and info
    print(f"\nSymbols: · = 1 job   • = 2-3 jobs   █ = 4+ jobs")
    if outliers > 0:
        print(f"Note: {outliers} job(s) with runtime > {format_seconds_human(p95_runtime)} (95th percentile) not shown")
    print()


def histogram(cluster_id, jobs, percentiles=10, max_width=20, show_fast_jobs=False):

    # Check if jobs list is empty
    if not jobs:
        print("[WARN] No valid data to plot.")
        return
    
    # Extract job runtime data and identifiers
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
    
    # Convert to numpy arrays for faster computation
    runtimes = np.array(runtimes)
    cluster_ids = np.array(cluster_ids)
    proc_ids = np.array(proc_ids)

    # Create evenly-spaced percentile boundaries (e.g., 0%, 10%, 20%, ..., 100%)
    # percentiles=10 creates 11 boundaries defining 10 bins
    percentiles_list = np.linspace(0, 100, percentiles + 1)
    
    # Calculate the actual runtime values at each percentile boundary
    bin_edges = np.percentile(runtimes, percentiles_list)
    
    # Count how many jobs fall into each percentile bin
    counts, _ = np.histogram(runtimes, bins=bin_edges)
    
    # Find the bin with the most jobs (used to scale the bar chart)
    max_count = counts.max()

    print(f"\n{'Histogram of Job Runtimes by Percentiles':^80}")
    print("=" * 80)
    print(f"ClusterId: {cluster_id}\n")

    # Display cluster submission and completion time range
    if submit_times:
        # Find when the first job in the cluster was submitted
        cluster_submit_time = format_epoch_human_relative(min(submit_times))
        print(f"First Submitted : {cluster_submit_time}")
    else:
        print("First Submitted : N/A")

    if completion_times:
        # Find when the last job in the cluster completed
        cluster_completion_time = format_epoch_human_relative(max(completion_times))
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


# function to load data from CSV file
def load_data_for_cluster(cluster_id):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        print(f"Cluster Data not found, please make sure you have the correct .csv, filepath and the correct cluster id")
        sys.exit(1)

    with open(filepath, newline='', encoding='utf-8') as f:
        jobs = list(csv.DictReader(f))
    
    return jobs


def get_histogram_data(cluster_id):
    """
    Return runtime analysis data as a dictionary for use by cluster_health.py
    Does not print anything, just returns computed metrics.
    
    Returns:
        dict: Dictionary containing runtime analysis metrics
    """
    # Load data without exiting on error (for use by cluster_health.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        return None

    with open(filepath, newline='', encoding='utf-8') as f:
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
    
    # Coefficient of variation
    cv = (std_runtime / mean_runtime) if mean_runtime > 0 else 0
    
    # Count fast jobs (< 10 minutes)
    fast_jobs = np.sum(runtimes < 600)
    fast_jobs_pct = (fast_jobs / len(runtimes)) * 100
    
    # Count very long jobs (> 95th percentile)
    p95 = np.percentile(runtimes, 95)
    long_jobs = np.sum(runtimes > p95)
    
    # Calculate correlation (job index vs runtime)
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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python histogram.py <clusterId> [printList]")
        sys.exit(1)

    cluster_id = sys.argv[1]
    print_list_flag = sys.argv[2].lower() in ("true", "yes", "1") if len(sys.argv) > 2 else False

    jobs = load_data_for_cluster(cluster_id)
    
    # Display scatter plot (smaller and more compact)
    scatter_plot_job_index_vs_runtime(cluster_id, jobs, height=15, width=60)
    
    # Then display histogram
    histogram(cluster_id, jobs, percentiles=10, max_width=20, show_fast_jobs=print_list_flag)

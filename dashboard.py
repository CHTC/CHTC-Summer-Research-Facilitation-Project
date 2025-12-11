import sys
import math
import htcondor2


"""
This program provides an ASCII dashboard for the status of jobs in a cluster
"""

# get data from the schedd
def fetch_counts(clusterId, job_states):
    schedd = htcondor2.Schedd()
    counts = { state: 0 for state in job_states }
    
    # history (finished jobs)
    print("Fetching job history (this may take a moment)...", file=sys.stderr)
    
    total_found = 0
    
    for i, ad in enumerate(schedd.history(
            constraint = f"ClusterId == {clusterId}",
            projection = ["JobStatus"],
            match = -1
        )):
        total_found += 1
        counts[job_states[ad.eval("JobStatus")-1]] += 1
        
        if total_found % 1000 == 0:
            print(f"  Found {total_found} matching jobs...", file=sys.stderr)
    
    print(f"  Completed: {total_found} matching jobs found", file=sys.stderr)
    
    # queue (running / pending jobs)
    print("Fetching current queue...", file=sys.stderr)
    for ad in schedd.query(
            constraint = f"ClusterId == {clusterId}",
            projection = ["JobStatus"],
            limit = -1
        ):
        counts[job_states[ad.eval("JobStatus")-1]] += 1
    print("Done fetching data\n", file=sys.stderr)
    
    return counts

#print the dashboard
def draw_bars(counts, job_states, bar_width=50):
    # compute column widths
    max_label_len = max(len(s) for s in job_states)
    max_count     = max(counts.values()) or 1
    count_width   = len(str(max_count))
    per_width     = len("100.0%")
    total_count   = sum(counts.values())

    # invalid cluster id
    if(total_count == 0 ):
        print("No jobs in the cluster found, please recheck clusterId")
        exit()


    # header
    header = (
        f"{'Status'.rjust(max_label_len)} | "
        f"{'Bar'.ljust(bar_width)} | "
        f"{'Count'.rjust(count_width)} | "
        f"{'%'.rjust(per_width)}"
    )
    print(header)
    print("-" * len(header))

    # rows
    for state in job_states:
        cnt    = counts[state]
        length = math.ceil( int(cnt / total_count * bar_width ) )
        bar    = "█" * length
        per    = cnt * 100 / total_count

        state_str = state.rjust(max_label_len)
        bar_str   = bar.ljust(bar_width)
        cnt_str   = str(cnt).rjust(count_width)
        per_str   = f"{per:5.1f}%".rjust(per_width)

        print(f"{state_str} | {bar_str} | {cnt_str} | {per_str}")


def get_dashboard_data(clusterId):
    """
    Return job status counts as a dictionary for use by cluster_health.py
    Does not print anything, just returns computed metrics.
    
    Returns:
        dict: Dictionary containing job status counts
    """
    job_states = [
        "Idle", "Running", "Removing", "Completed",
        "Held", "Transferring Output", "Suspended"
    ]
    
    try:
        counts = fetch_counts(clusterId, job_states)
        total = sum(counts.values())
        
        if total == 0:
            return None
        
        return {
            "total_jobs": total,
            "status_counts": counts,
            "completed": counts.get("Completed", 0),
            "held": counts.get("Held", 0),
            "running": counts.get("Running", 0),
            "idle": counts.get("Idle", 0),
            "held_pct": (counts.get("Held", 0) / total) * 100 if total > 0 else 0,
            "completed_pct": (counts.get("Completed", 0) / total) * 100 if total > 0 else 0,
        }
    except Exception as e:
        return None



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dashboard.py <ClusterId>")
        sys.exit(1)

    clusterId  = sys.argv[1]
    job_states = [
        "Idle", "Running", "Removing", "Completed",
        "Held", "Transferring Output", "Suspended"
    ]

    counts = fetch_counts(clusterId, job_states)
    print(f"\nCluster {clusterId} Status Dashboard\n")
    draw_bars(counts, job_states)

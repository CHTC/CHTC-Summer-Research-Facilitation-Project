import os
import sys
import csv
import statistics
from collections import Counter
from datetime import timedelta
from utils import safe_float

"""
This program provides a report on the resource request and usage for a cluster

"""


# to print the bar visualizations
def bar(pct, width=50):
    filled = int(pct / 100 * width)
    return "[" + "█" * filled + " " * (width - filled) + f"] {pct:.1f}%"

# to calculate efficiency
def efficiency(used, expected):
    if not expected:
        return 0.0
    return (used / expected) * 100

# to print the usage report
def compute_usage_summary(data, label, percentage=False, unit=None):
    if not data or len(data) < 2:
        return f"{label:<25}: Not enough data"

    data_sorted = sorted(data)
    min_val = data_sorted[0]
    q1 = statistics.quantiles(data_sorted, n=4)[0]
    median = statistics.median(data_sorted)
    q3 = statistics.quantiles(data_sorted, n=4)[2]
    max_val = data_sorted[-1]
    std_dev = statistics.stdev(data_sorted)

    fmt = "{:.1f}%%" if percentage else "{:.1f}"
    return (
        f"{label:<25}: "
        f"{fmt.format(min_val):>6}  {fmt.format(q1):>6}  {fmt.format(median):>7}  "
        f"{fmt.format(q3):>6}  {fmt.format(max_val):>6}   {fmt.format(std_dev):>6}"
    )

# prints the resource request table
def print_resource_table(name, values, unit=""):
    if not values:
        print(f"{name:<15}: No data")
        return

    counts = Counter(values)
    print(f"{name:<15}:")
    for val, count in sorted(counts.items()):
        print(f"{'':<15}  {val:<10} {unit:<5}  {count} job(s)")
    print()

# prints the total report
def summarize(cluster_id):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        sys.exit(1)

    with open(filepath, newline='', encoding='utf-8') as f:
        jobs = list(csv.DictReader(f))

    mem_requested, mem_used = [], []
    disk_requested, disk_used = [], []
    run_time, cpu_used_time = [], []
    runtimes = []
    cpu_requests = []
    gpu_requests = []

    for job in jobs:
        mem_req = safe_float(job.get("RequestMemory"))
        mem_use = safe_float(job.get("ResidentSetSize_RAW"))
        if mem_req:
            mem_requested.append(round(mem_req / 1024, 2))  # Convert MiB to GiB
        if mem_use:
            mem_used.append(mem_use / 1024 / 1024)  # Convert KiB to GiB

        disk_req = safe_float(job.get("RequestDisk"))
        disk_use = safe_float(job.get("DiskUsage_RAW"))
        if disk_req:
            disk_requested.append(round(disk_req / (1024 * 1024), 2))  # Convert KiB to GiB
        if disk_use:
            disk_used.append(disk_use / (1024 * 1024))  # Convert KiB to GiB

        cpus = safe_float(job.get("RequestCpus"))
        if cpus:
            cpu_requests.append(int(cpus))

        gpus = safe_float(job.get("RequestGpus"))
        if gpus:
            gpu_requests.append(int(gpus))

        user_cpu = safe_float(job.get("RemoteUserCpu")) or 0
        sys_cpu = safe_float(job.get("RemoteSysCpu")) or 0
        wall_time = safe_float(job.get("RemoteWallClockTime"))

        if wall_time and cpus and (user_cpu or sys_cpu):
            total_cpu_used = sys_cpu / cpus
            cpu_used_time.append(total_cpu_used)
            run_time.append(wall_time)

        if wall_time:
            runtimes.append(wall_time)

    from statistics import median

    # Compute per-job efficiency lists
    per_job_cpu_eff = [
        efficiency(cpu_used_time[i], run_time[i])
        for i in range(len(cpu_used_time))
        if run_time[i]
    ]

    per_job_mem_eff = [
        efficiency(mem_used[i], mem_requested[i])
        for i in range(min(len(mem_used), len(mem_requested)))
        if mem_requested[i]
    ]

    per_job_disk_eff = [
        efficiency(disk_used[i], disk_requested[i])
        for i in range(min(len(disk_used), len(disk_requested)))
        if disk_requested[i]
    ]

    # Take medians
    avg_cpu_eff = median(per_job_cpu_eff) if per_job_cpu_eff else 0
    avg_mem_eff = median(per_job_mem_eff) if per_job_mem_eff else 0
    avg_disk_eff = median(per_job_disk_eff) if per_job_disk_eff else 0

    
    total_jobs = len(jobs)
    avg_runtime = statistics.mean(runtimes) if runtimes else 0
    avg_runtime_str = str(timedelta(seconds=int(avg_runtime))) if avg_runtime else "N/A"

    print("=" * 80)
    print(f"{'HTCondor Cluster Resource Summary':^80}")
    print("=" * 80)
    print(f"{'Cluster ID':>20}: {cluster_id}")
    print(f"{'Job Count':>20}: {total_jobs}")
    print(f"{'Avg Runtime':>20}: {avg_runtime_str}")
    print()

    print(f"{'Requested Resources':^80}")
    print("=" * 80)
    print_resource_table("Memory (GiB)", mem_requested, "GiB")
    print_resource_table("Disk (GiB)", disk_requested, "GiB")
    print_resource_table("CPUs", cpu_requests, "")
    print_resource_table("GPUs", gpu_requests, "")

    print(f"{'Number Summary Table':^80}")
    print("=" * 80)
    print(f"{'Resource (units)':<25}: {'Min':>6}  {'Q1':>6}  {'Median':>7}  {'Q3':>6}  {'Max':>6}   {'StdDev':>6}")
    print("-" * 80)

    cpu_usages, mem_values, disk_values = [], [], []

    for i in range(len(jobs)):
        if i < len(cpu_used_time) and i < len(run_time) and run_time[i]:
            cpu_usages.append(efficiency(cpu_used_time[i], run_time[i]))
        if i < len(mem_used):
            mem_values.append(mem_used[i])
        if i < len(disk_used):
            disk_values.append(disk_used[i])


    print(compute_usage_summary(mem_values, "Memory Used (GiB)"))
    print(compute_usage_summary(disk_values, "Disk Used (GiB)"))
    print(compute_usage_summary(cpu_usages, "CPU Usage (%)", percentage=True))
    

    print()

    print(f"{'Overall Utilization':^80}")
    print("=" * 80)
    print(f"  Memory usage      {bar(avg_mem_eff)}")
    print(f"  Disk usage        {bar(avg_disk_eff)}")
    print(f"  CPU usage         {bar(avg_cpu_eff)}")
    print()


    # Gives human readable notes on the efficiency and also warnings
    print(f"{'Efficiency Notes':^80}")
    print("=" * 80)

    def warn(resource, efficiency):
        if efficiency < 15 or efficiency > 80:
            print(f"  ⚠️  {resource} usage is {efficiency:.1f}%")
        else:
            print(f"  ✅ {resource} usage is {efficiency:.1f}%")

    warn("Memory", avg_mem_eff)
    warn("Disk", avg_disk_eff)
    warn("CPU", avg_cpu_eff)


    print()
    print(f"{'End of Summary':^80}")
    print("=" * 80)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python htcondor_cluster_summary.py <ClusterId>")
        sys.exit(1)
    summarize(sys.argv[1])

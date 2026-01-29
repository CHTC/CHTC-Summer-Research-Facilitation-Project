import os
import sys
import csv
import statistics
from collections import Counter, defaultdict
from datetime import timedelta
from utils import safe_float
import argparse


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

# to calculate waste
def calculate_waste(requested, used):
    if not requested:
        return 0.0
    return max(0, requested - used)

# to get percentile value
def percentile(data, p):
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])

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
    
    fmt = "{:.1f}%" if percentage else "{:.1f}"
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

# prints distribution of jobs by actual resource usage as a histogram
def print_usage_distribution(name, used_list, unit="GiB"):
    if not used_list:
        return
    
    max_val = max(used_list)
    
    # Define bins based on the data range
    if max_val <= 10:
        bins = [0, 2, 5, 10, float('inf')]
        labels = ["0-2", "2-5", "5-10", "10+"]
    elif max_val <= 50:
        bins = [0, 5, 10, 20, 50, float('inf')]
        labels = ["0-5", "5-10", "10-20", "20-50", "50+"]
    else:
        bins = [0, 10, 25, 50, 100, float('inf')]
        labels = ["0-10", "10-25", "25-50", "50-100", "100+"]
    
    # Count jobs in each bin
    bin_counts = [0] * len(labels)
    for val in used_list:
        for i in range(len(bins) - 1):
            if bins[i] <= val < bins[i+1]:
                bin_counts[i] += 1
                break
    
    total_jobs = len(used_list)
    
    print(f"\n{name} Distribution:")
    
    # Find max count for scaling
    max_count = max(bin_counts) if bin_counts else 1
    bar_width = 50
    
    for i, (label, count) in enumerate(zip(labels, bin_counts)):
        if count > 0:
            pct = (count / total_jobs) * 100
            # Create histogram bar
            bar_length = int((count / max_count) * bar_width)
            bar_visual = "█" * bar_length
            print(f"  {label:>10} {unit}: {bar_visual:<{bar_width}} {count:>4} ({pct:>5.1f}%)")
        else:
            # Show empty bins if they exist
            print(f"  {label:>10} {unit}: {'':<{bar_width}} {count:>4} (  0.0%)")

# print recommendations
def print_recommendations(mem_req, mem_used, disk_req, disk_used, cpu_req, cpu_used_pct, avg_runtime_hours):
    print(f"\n{'Resource Optimization Recommendations':^80}")
    print("=" * 80)
    
    if mem_req and mem_used:
        p95_mem = percentile(mem_used, 95)
        recommended_mem = p95_mem * 1.1  # 10% buffer
        median_mem_req = statistics.median(mem_req)
        
        if recommended_mem < median_mem_req * 0.8:  # If we can save >20%
            savings = (median_mem_req - recommended_mem) * len(mem_used) * avg_runtime_hours
            print(f"\n📊 Memory:")
            print(f"  Current Request     : {median_mem_req:.1f} GiB")
            print(f"  Recommended         : {recommended_mem:.1f} GiB (P95 + 10% buffer)")
            print(f"  Potential Savings   : {savings:.1f} GiB-hours")
            print(f"  Jobs Affected       : {len(mem_used)}")
    
    if disk_req and disk_used:
        p95_disk = percentile(disk_used, 95)
        recommended_disk = p95_disk * 1.2  # 20% buffer for disk
        median_disk_req = statistics.median(disk_req)
        
        if recommended_disk < median_disk_req * 0.8:
            savings = (median_disk_req - recommended_disk) * len(disk_used) * avg_runtime_hours
            print(f"\n💾 Disk:")
            print(f"  Current Request     : {median_disk_req:.1f} GiB")
            print(f"  Recommended         : {recommended_disk:.1f} GiB (P95 + 20% buffer)")
            print(f"  Potential Savings   : {savings:.1f} GiB-hours")
            print(f"  Jobs Affected       : {len(disk_used)}")
    
    if cpu_req and cpu_used_pct:
        median_cpu_pct = statistics.median(cpu_used_pct)
        median_cpu_req = statistics.median(cpu_req)
        
        if median_cpu_pct < 50:
            # Calculate recommended CPUs based on actual usage
            # Use the median efficiency to scale down the request
            recommended_cpus = max(1, int(median_cpu_req * (median_cpu_pct / 100) * 1.2))  # 20% buffer
            
            print(f"\n⚙️  CPU:")
            print(f"  Current Request     : {median_cpu_req:.1f} CPUs")
            print(f"  Current Efficiency  : {median_cpu_pct:.1f}%")
            print(f"  Recommended         : {recommended_cpus} CPUs")
            print(f"  Jobs Affected       : {len(cpu_used_pct)}")

# prints the total report
def summarize(cluster_id):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        print(f"Cluster Data not found, please make sure you have the correct .csv, filepath and the correct cluster id")
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
    avg_cpu_eff = statistics.median(per_job_cpu_eff) if per_job_cpu_eff else 0
    avg_mem_eff = statistics.median(per_job_mem_eff) if per_job_mem_eff else 0
    avg_disk_eff = statistics.median(per_job_disk_eff) if per_job_disk_eff else 0

    
    total_jobs = len(jobs)
    avg_runtime = statistics.mean(runtimes) if runtimes else 0
    avg_runtime_str = str(timedelta(seconds=int(avg_runtime))) if avg_runtime else "N/A"
    avg_runtime_hours = avg_runtime / 3600 if avg_runtime else 1.0

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

    # Usage distribution
    print(f"{'Resource Usage Distribution':^80}")
    print("=" * 80)
    print_usage_distribution("Memory", mem_used, "GiB")
    print_usage_distribution("Disk", disk_used, "GiB")
    
    # Recommendations
    print_recommendations(mem_requested, mem_used, disk_requested, disk_used, 
                         cpu_requests, cpu_usages, avg_runtime_hours)

    # Gives human readable notes on the efficiency and also warnings
    print()
    print(f"{'Efficiency Summary':^80}")
    print("=" * 80)

    def warn(resource, efficiency):
        if efficiency < 15:
            print(f"  ⚠️  {resource} usage is {efficiency:.1f}% - significant over-provisioning")
        elif efficiency < 50:
            print(f"  ⚠️  {resource} usage is {efficiency:.1f}% - consider reducing requests")
        elif efficiency > 80:
            print(f"  ✅ {resource} usage is {efficiency:.1f}% - well optimized")
        else:
            print(f"  ✅ {resource} usage is {efficiency:.1f}%")

    warn("Memory", avg_mem_eff)
    warn("Disk", avg_disk_eff)
    warn("CPU", avg_cpu_eff)


    print()
    print(f"{'End of Summary':^80}")
    print("=" * 80)


def get_analytics_data(cluster_id):
    """
    Return analytics data as a dictionary for use by cluster_health.py
    Does not print anything, just returns computed metrics.
    
    Returns:
        dict: Dictionary containing all analytics metrics
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        return None

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
            mem_requested.append(round(mem_req / 1024, 2))
        if mem_use:
            mem_used.append(mem_use / 1024 / 1024)

        disk_req = safe_float(job.get("RequestDisk"))
        disk_use = safe_float(job.get("DiskUsage_RAW"))
        if disk_req:
            disk_requested.append(round(disk_req / (1024 * 1024), 2))
        if disk_use:
            disk_used.append(disk_use / (1024 * 1024))

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
    avg_cpu_eff = statistics.median(per_job_cpu_eff) if per_job_cpu_eff else 0
    avg_mem_eff = statistics.median(per_job_mem_eff) if per_job_mem_eff else 0
    avg_disk_eff = statistics.median(per_job_disk_eff) if per_job_disk_eff else 0

    avg_runtime = statistics.mean(runtimes) if runtimes else 0
    avg_runtime_hours = avg_runtime / 3600 if avg_runtime else 1.0

    # Calculate savings
    savings = {}
    
    if mem_requested and mem_used:
        p95_mem = percentile(mem_used, 95)
        recommended_mem = p95_mem * 1.1
        median_mem_req = statistics.median(mem_requested)
        
        if recommended_mem < median_mem_req * 0.8:
            mem_savings = (median_mem_req - recommended_mem) * len(mem_used) * avg_runtime_hours
            savings["memory"] = {
                "current": median_mem_req,
                "recommended": recommended_mem,
                "savings_gib_hours": mem_savings,
                "reduction_pct": ((median_mem_req - recommended_mem) / median_mem_req) * 100
            }
    
    if disk_requested and disk_used:
        p95_disk = percentile(disk_used, 95)
        recommended_disk = p95_disk * 1.2
        median_disk_req = statistics.median(disk_requested)
        
        if recommended_disk < median_disk_req * 0.8:
            disk_savings = (median_disk_req - recommended_disk) * len(disk_used) * avg_runtime_hours
            savings["disk"] = {
                "current": median_disk_req,
                "recommended": recommended_disk,
                "savings_gib_hours": disk_savings,
                "reduction_pct": ((median_disk_req - recommended_disk) / median_disk_req) * 100
            }
    
    if cpu_requests and per_job_cpu_eff:
        median_cpu_pct = statistics.median(per_job_cpu_eff)
        median_cpu_req = statistics.median(cpu_requests)
        
        if median_cpu_pct < 50:
            recommended_cpus = max(1, int(median_cpu_req * (median_cpu_pct / 100) * 1.2))
            savings["cpu"] = {
                "current": median_cpu_req,
                "recommended": recommended_cpus,
                "current_efficiency": median_cpu_pct,
            }

    return {
        "total_jobs": len(jobs),
        "avg_runtime": avg_runtime,
        "avg_runtime_hours": avg_runtime_hours,
        "memory_efficiency": avg_mem_eff,
        "disk_efficiency": avg_disk_eff,
        "cpu_efficiency": avg_cpu_eff,
        "memory_jobs": len(per_job_mem_eff),
        "disk_jobs": len(per_job_disk_eff),
        "cpu_jobs": len(per_job_cpu_eff),
        "mem_requested": mem_requested,
        "mem_used": mem_used,
        "disk_requested": disk_requested,
        "disk_used": disk_used,
        "cpu_requests": cpu_requests,
        "cpu_efficiency_list": per_job_cpu_eff,
        "savings": savings,
    }


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                    prog='analytics',
                    description='The program provides analytics of specific cluster',
                    epilog='')
    
    parser.add_argument('-cluster_id',required=True)    
    args = parser.parse_args()

    summarize(args.cluster_id)

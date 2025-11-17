import os
import csv
import sys
from tabulate import tabulate
from utils import safe_float

"""
This Python script prints a summary table for a given cluster's jobs
using selected parameters from the corresponding CSV in 'cluster_data/'.
It supports command-line parameter selection and auto-converts RAW values to GiB.
"""

# finds the cluster data from the folder based on the clusterId
def load_job_data(cluster_id, folder="cluster_data"):
    filepath = os.path.join(folder, f"cluster_{cluster_id}_jobs.csv")
    if not os.path.exists(filepath):
        print(f"Cluster Data not found, please make sure you have the correct .csv, filepath and the correct cluster id ")
        sys.exit(1)

    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        jobs = list(reader)

    return jobs

# check if all selected parameters exist in the CSV header
def validate_params(jobs, selected_params):
    if not jobs:
        return

    csv_keys = jobs[0].keys()
    missing = [param for param in selected_params if param not in csv_keys]

    if missing:
        print(f"Error: The following parameters were not found in the CSV data: {', '.join(missing)}")
        print(f"Available columns: {', '.join(csv_keys)}")
        sys.exit(1)

# extract selected parameter values for each job
def extract_requested_vs_used(jobs, selected_params):
    rows = []
    for job in jobs:
        row = {
            "ProcId": job.get("ProcId")
        }
        for param in selected_params:
            raw_val = job.get(param)
            val = safe_float(raw_val)

            # Use original value if not a float 
            if val is None:
                val = raw_val
            elif "RAW" in param:
                val = val / 1024  # Convert MiB to GiB

            row[param] = val

        rows.append(row)
    return rows

# main execution logic
def main():
    if len(sys.argv) < 2:
        print("Usage: python summarise.py <ClusterId> [param1 param2 ...]")
        sys.exit(1)

    cluster_id = sys.argv[1]

    # default list of attributes to summarise if none have been passed
    selected_params = sys.argv[2:] if len(sys.argv) > 2 else [
        "RequestCpus", "CpusProvisioned", "RemoteSysCpu", 
        "RemoteUserCpu", "RemoteWallClockTime", "ResidentSetSize_RAW"
    ]

    jobs = load_job_data(cluster_id)
    validate_params(jobs, selected_params)
    data = extract_requested_vs_used(jobs, selected_params)
    total_jobs = len(data)

    if not data:
        print("No valid job data found.")
        return

    print(f"\nSummary for ClusterId: {cluster_id}")
    print(f"Total Jobs: {total_jobs}\n")
    print(tabulate(data, headers="keys", tablefmt="grid", floatfmt=".2f"))

if __name__ == "__main__":
    main()

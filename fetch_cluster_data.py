import os
import csv
import sys
import argparse
import htcondor2
from datetime import datetime

"""
This program fetches all job data from HTCondor history for a given cluster
and stores it in a CSV file for analysis by other tools in this package.
"""

# All parameters needed by the analytics suite
REQUIRED_PARAMS = [
    # Job identifiers
    "ClusterId",
    "ProcId",
    "JobStatus",

    # Resource requests
    "RequestMemory",
    "RequestDisk",
    "RequestCpus",
    "RequestGpus",

    # Resource usage (RAW values in KiB)
    "ResidentSetSize_RAW",
    "DiskUsage_RAW",

    # CPU usage (in seconds)
    "RemoteUserCpu",
    "RemoteSysCpu",
    "RemoteWallClockTime",

    # Provisioned resources
    "CpusProvisioned",

    # Hold information
    "HoldReason",
    "HoldReasonCode",
    "HoldReasonSubCode",

    # Timing information
    "QDate",
    "CompletionDate",
    "JobStartDate",
    "EnteredCurrentStatus",
]


def fetch_cluster_jobs(cluster_id, output_dir="cluster_data"):
    """
    Fetch all jobs from HTCondor history for a given cluster and save to CSV.

    Parameters:
        cluster_id (str or int): The cluster ID to fetch
        output_dir (str): Directory to save the CSV file

    Returns:
        tuple: (filepath, job_count) - path to created CSV and number of jobs fetched

    Raises:
        ValueError: If cluster_id cannot be interpreted as an integer.
    """
    try:
        cluster_id = int(cluster_id)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid cluster ID {cluster_id!r}: must be an integer.")

    schedd = htcondor2.Schedd()

    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, f"cluster_{cluster_id}_jobs.csv")

    print(f"Fetching jobs for cluster {cluster_id}...")
    print(f"This may take a moment for large clusters...\n")

    jobs_data = []
    job_count = 0

    print("Querying job history...", file=sys.stderr)
    try:
        for i, ad in enumerate(schedd.history(
            constraint=f"ClusterId == {cluster_id}",
            projection=REQUIRED_PARAMS,
            match=-1
        )):
            job_dict = {}
            for param in REQUIRED_PARAMS:
                try:
                    value = ad.get(param, None)
                    if value is None:
                        try:
                            value = ad.eval(param)
                        except:
                            value = None
                    job_dict[param] = value if value is not None else ""
                except:
                    job_dict[param] = ""

            jobs_data.append(job_dict)
            job_count += 1

            if job_count % 1000 == 0:
                print(f"  Fetched {job_count} jobs from history...", file=sys.stderr)

        print(f"  History complete: {job_count} jobs", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Error querying history: {e}", file=sys.stderr)

    print("Querying current queue...", file=sys.stderr)
    queue_count = 0
    try:
        for ad in schedd.query(
            constraint=f"ClusterId == {cluster_id}",
            projection=REQUIRED_PARAMS,
            limit=-1
        ):
            job_dict = {}
            for param in REQUIRED_PARAMS:
                try:
                    value = ad.get(param, None)
                    if value is None:
                        try:
                            value = ad.eval(param)
                        except:
                            value = None
                    job_dict[param] = value if value is not None else ""
                except:
                    job_dict[param] = ""

            jobs_data.append(job_dict)
            job_count += 1
            queue_count += 1

        print(f"  Queue complete: {queue_count} jobs", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Error querying queue: {e}", file=sys.stderr)

    if job_count == 0:
        print(f"\nError: No jobs found for cluster {cluster_id}")
        print("Please verify the cluster ID is correct.")
        sys.exit(1)

    print(f"\nWriting {job_count} jobs to CSV...", file=sys.stderr)
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=REQUIRED_PARAMS)
            writer.writeheader()
            writer.writerows(jobs_data)

        print(f"✓ Successfully saved data to: {filepath}")
        print(f"✓ Total jobs fetched: {job_count}")

        status_counts = {}
        status_names = {
            1: "Idle",
            2: "Running",
            3: "Removing",
            4: "Completed",
            5: "Held",
            6: "Transferring",
            7: "Suspended"
        }

        for job in jobs_data:
            status = job.get("JobStatus", "")
            if status:
                try:
                    status_int = int(status)
                    status_name = status_names.get(status_int, f"Unknown({status_int})")
                    status_counts[status_name] = status_counts.get(status_name, 0) + 1
                except:
                    pass

        if status_counts:
            print("\nJob Status Breakdown:")
            for status, count in sorted(status_counts.items()):
                print(f"  {status:<15}: {count:>6} jobs")

        return filepath, job_count

    except Exception as e:
        print(f"Error writing CSV: {e}", file=sys.stderr)
        sys.exit(1)


def validate_cluster_exists(cluster_id):
    """
    Quick check to see if cluster exists before full fetch.

    Parameters:
        cluster_id (str or int): The cluster ID to validate

    Returns:
        bool: True if cluster has jobs, False otherwise

    Raises:
        ValueError: If cluster_id cannot be interpreted as an integer.
    """
    try:
        cluster_id = int(cluster_id)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid cluster ID {cluster_id!r}: must be an integer.")

    schedd = htcondor2.Schedd()

    try:
        for ad in schedd.history(
            constraint=f"ClusterId == {cluster_id}",
            projection=["ClusterId"],
            match=1
        ):
            return True

        for ad in schedd.query(
            constraint=f"ClusterId == {cluster_id}",
            projection=["ClusterId"],
            limit=1
        ):
            return True

        return False
    except:
        return False


def parse_args():
    parser = argparse.ArgumentParser(
        prog="fetch_cluster_data",
        description="Fetches all job data for a cluster from HTCondor and saves to CSV.",
        epilog="Output: cluster_data/cluster_<CLUSTER_ID>_jobs.csv",
    )
    parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        help="HTCondor cluster ID to fetch.",
    )
    parser.add_argument(
        "output_dir",
        metavar="OUTPUT_DIR",
        nargs="?",
        default="cluster_data",
        help="Directory to save the CSV file (default: cluster_data).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    cluster_id = args.cluster_id
    output_dir = args.output_dir

    print("=" * 80)
    print(f"{'HTCondor Cluster Data Fetch':^80}")
    print("=" * 80)
    print(f"Cluster ID    : {cluster_id}")
    print(f"Output Dir    : {output_dir}")
    print(f"Timestamp     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()

    print("Validating cluster ID...", file=sys.stderr)
    if not validate_cluster_exists(cluster_id):
        print(f"\nError: No jobs found for cluster {cluster_id}")
        print("Please verify:")
        print("  1. The cluster ID is correct")
        print("  2. You have permission to access this cluster")
        print("  3. The cluster exists in HTCondor history or queue")
        sys.exit(1)

    print("✓ Cluster found\n", file=sys.stderr)

    filepath, job_count = fetch_cluster_jobs(cluster_id, output_dir)

    print("\n" + "=" * 80)
    print("Data fetch complete!")
    print("=" * 80)
    print(f"\nYou can now analyse this cluster with:")
    print(f"  python main.py analytics  {cluster_id}")
    print(f"  python main.py histogram  {cluster_id}")
    print(f"  python main.py summarize  {cluster_id}")
    print(f"  python main.py dashboard  {cluster_id}")
    print(f"  python main.py hold       {cluster_id}")
    print()


if __name__ == "__main__":
    main()
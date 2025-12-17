import sys
import htcondor2
from difflib import SequenceMatcher
from tabulate import tabulate
import argparse
import datetime
import time as time_module


"""
This program buckets and tabulates the held jobs for a cluster

"""

# Mapping of HoldReasonCodes to their explanations
HOLD_REASON_CODES = {
    1: {"label": "UserRequest", "reason": "The user put the job on hold with condor_hold."},
    3: {"label": "JobPolicy", "reason": "The PERIODIC_HOLD expression evaluated to True. Or, ON_EXIT_HOLD was true."},
    4: {"label": "CorruptedCredential", "reason": "The credentials for the job are invalid."},
    5: {"label": "JobPolicyUndefined", "reason": "A job policy expression evaluated to Undefined."},
    6: {"label": "FailedToCreateProcess", "reason": "The condor_starter failed to start the executable."},
    7: {"label": "UnableToOpenOutput", "reason": "The standard output file for the job could not be opened."},
    8: {"label": "UnableToOpenInput", "reason": "The standard input file for the job could not be opened."},
    9: {"label": "UnableToOpenOutputStream", "reason": "The standard output stream for the job could not be opened."},
    10: {"label": "UnableToOpenInputStream", "reason": "The standard input stream for the job could not be opened."},
    11: {"label": "InvalidTransferAck", "reason": "An internal HTCondor protocol error was encountered when transferring files."},
    12: {"label": "TransferOutputError", "reason": "An error occurred while transferring job output files or self-checkpoint files."},
    13: {"label": "TransferInputError", "reason": "An error occurred while transferring job input files."},
    14: {"label": "IwdError", "reason": "The initial working directory of the job cannot be accessed."},
    15: {"label": "SubmittedOnHold", "reason": "The user requested the job be submitted on hold."},
    16: {"label": "SpoolingInput", "reason": "Input files are being spooled."},
    17: {"label": "JobShadowMismatch", "reason": "A standard universe job is not compatible with the condor_shadow version available on the submitting machine."},
    18: {"label": "InvalidTransferGoAhead", "reason": "An internal HTCondor protocol error was encountered when transferring files."},
    19: {"label": "HookPrepareJobFailure", "reason": "<Keyword>_HOOK_PREPARE_JOB was defined but could not be executed or returned failure."},
    20: {"label": "MissedDeferredExecutionTime", "reason": "The job missed its deferred execution time and therefore failed to run."},
    21: {"label": "StartdHeldJob", "reason": "The job was put on hold because WANT_HOLD in the machine policy was true."},
    22: {"label": "UnableToInitUserLog", "reason": "Unable to initialize job event log."},
    23: {"label": "FailedToAccessUserAccount", "reason": "Failed to access user account."},
    24: {"label": "NoCompatibleShadow", "reason": "No compatible shadow."},
    25: {"label": "InvalidCronSettings", "reason": "Invalid cron settings."},
    26: {"label": "SystemPolicy", "reason": "SYSTEM_PERIODIC_HOLD evaluated to true."},
    27: {"label": "SystemPolicyUndefined", "reason": "The system periodic job policy evaluated to undefined."},
    32: {"label": "MaxTransferInputSizeExceeded", "reason": "The maximum total input file transfer size was exceeded."},
    33: {"label": "MaxTransferOutputSizeExceeded", "reason": "The maximum total output file transfer size was exceeded."},
    34: {"label": "JobOutOfResources", "reason": "Memory usage exceeds a memory limit."},
    35: {"label": "InvalidDockerImage", "reason": "Specified Docker image was invalid."},
    36: {"label": "FailedToCheckpoint", "reason": "Job failed when sent the checkpoint signal it requested."},
    43: {"label": "PreScriptFailed", "reason": "Pre script failed."},
    44: {"label": "PostScriptFailed", "reason": "Post script failed."},
    45: {"label": "SingularityTestFailed", "reason": "Test of singularity runtime failed before launching a job"},
    46: {"label": "JobDurationExceeded", "reason": "The job's allowed duration was exceeded."},
    47: {"label": "JobExecuteExceeded", "reason": "The job's allowed execution time was exceeded."},
    48: {"label": "HookShadowPrepareJobFailure", "reason": "Prepare job shadow hook failed when it was executed; status code indicated job should be held."}
}

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Analyze and categorize held jobs in an HTCondor cluster',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Basic usage:
    %(prog)s 4641492
    
  Filter and sort:
    %(prog)s 4641492 --min-count 10 --sort-by time
    %(prog)s 4641492 --top 5 --sort-by percent
    %(prog)s 4641492 --code 34 --threshold 0.8
    
  Export for bulk operations:
    %(prog)s 4641492 --export-jobs held.txt
    condor_release $(cat held.txt)
    
  Advanced analysis:
    %(prog)s 4641492 --show-job-ids --sort-by time --min-count 5
    %(prog)s 4641492 --code 12 --export-jobs output_errors.txt
  
For full list of hold codes, see the output legend or:
  https://htcondor.readthedocs.io/en/latest/
        """
    )
    
    parser.add_argument(
        'cluster_id', 
        help='HTCondor cluster ID to analyze (required)'
    )
    
    # Filtering options
    filter_group = parser.add_argument_group('filtering options')
    filter_group.add_argument(
        '--min-count', 
        type=int, 
        default=1,
        metavar='N',
        help='only show error buckets with at least N jobs (default: 1). '
             'Use this to filter out rare errors. Example: --min-count 10'
    )
    filter_group.add_argument(
        '--top', 
        type=int,
        metavar='N',
        help='show only the top N most common error buckets. '
             'Useful for focusing on major issues. Example: --top 5'
    )
    filter_group.add_argument(
        '--code', 
        type=int,
        metavar='CODE',
        help='filter results to show only jobs with specific HoldReasonCode. '
             'Common codes: 3 (JobPolicy), 34 (Memory), 12 (Output Transfer), '
             '13 (Input Transfer). Example: --code 34'
    )
    
    # Sorting options
    sort_group = parser.add_argument_group('sorting options')
    sort_group.add_argument(
        '--sort-by', 
        choices=['count', 'code', 'percent', 'time'], 
        default='count',
        help='sort results by different criteria:\n'
             '  count   - number of jobs (default, most common first)\n'
             '  code    - hold reason code (numerical order)\n'
             '  percent - percentage of total held jobs\n'
             '  time    - average hold duration (longest first)\n'
             'Example: --sort-by time'
    )
    
    # Bucketing options
    bucket_group = parser.add_argument_group('bucketing options')
    bucket_group.add_argument(
        '--threshold', 
        type=float, 
        default=0.7,
        metavar='RATIO',
        help='similarity threshold (0.0-1.0) for grouping similar error messages. '
             'Higher values = stricter matching (more buckets). '
             'Lower values = looser matching (fewer, larger buckets). '
             'Default: 0.7. Try 0.8 for stricter or 0.6 for looser grouping. '
             'Example: --threshold 0.8'
    )
    
    # Output options
    output_group = parser.add_argument_group('output options')
    output_group.add_argument(
        '--show-job-ids', 
        action='store_true',
        help='display ProcIds in the output table. Useful for identifying '
             'which specific jobs are affected. Note: only shows first few IDs '
             'for large buckets to keep output readable'
    )
    output_group.add_argument(
        '--export-jobs', 
        metavar='FILENAME',
        help='export all held job IDs to a file for bulk operations. '
             'The file will contain one job ID per line in format ClusterId.ProcId. '
             'Use with condor_release or condor_rm for batch processing. '
             'Example: --export-jobs held.txt'
    )
    
    return parser.parse_args()


"""
Groups similar hold reason messages using fuzzy string matching (difflib.SequenceMatcher).

    Parameters:
        reason_list (List[Tuple[str, int, int]]): List of (reason, subcode, proc_id) tuples.
        threshold (float): Similarity ratio (between 0 and 1) above which reasons are considered similar.

    Returns:
        List[List[Tuple[str, int, int, int]]]: Buckets of (reason, subcode, proc_id, hold_time) tuples.
"""
def bucket_reasons_with_data(reason_data, threshold=0.7):
    buckets = []
    for reason, subcode, proc_id, hold_time in reason_data:
        placed = False
        for bucket in buckets:
            ratio = SequenceMatcher(None, reason, bucket[0][0]).ratio()
            if ratio >= threshold:
                bucket.append((reason, subcode, proc_id, hold_time))
                placed = True
                break
        if not placed:
            buckets.append([(reason, subcode, proc_id, hold_time)])
    return buckets


def format_duration(seconds):
    """Format duration in a human-readable way"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    elif seconds < 86400:
        hours = seconds / 3600
        return f"{hours:.1f}h"
    else:
        days = seconds / 86400
        return f"{days:.1f}d"


def calculate_avg_hold_time(bucket):
    """Calculate average time jobs have been held in a bucket"""
    current_time = time_module.time()
    hold_durations = []
    
    for _, _, _, hold_time in bucket:
        if hold_time > 0:
            duration = current_time - hold_time
            hold_durations.append(duration)
    
    if not hold_durations:
        return None, "N/A"
    
    avg_seconds = sum(hold_durations) / len(hold_durations)
    return avg_seconds, format_duration(avg_seconds)


""" 
Queries the HTCondor schedd for held jobs in the specified cluster and groups them by their HoldReasonCode.
Now also collects ProcId and EnteredCurrentStatus (hold time).

    Parameters:
        cluster_id (str or int): The ID of the cluster to analyze.

    Returns:
        Dict[int, List[Tuple[str, int, int, int]]]: Maps HoldReasonCode to list of 
                                                      (HoldReason, HoldReasonSubCode, ProcId, HoldTime) tuples.
"""
def group_by_code(cluster_id):
    schedd = htcondor2.Schedd()
    reasons_by_code = {}

    print("Fetching held jobs from cluster...", file=sys.stderr)
    
    for ad in schedd.query(
        constraint=f"ClusterId == {cluster_id} && JobStatus == 5",
        projection=["ProcId", "HoldReasonCode", "HoldReason", "HoldReasonSubCode", "EnteredCurrentStatus"],
        limit=-1
    ):
        code = ad.eval("HoldReasonCode")
        subcode = ad.eval("HoldReasonSubCode")
        proc_id = ad.eval("ProcId")
        hold_time = ad.get("EnteredCurrentStatus", 0)

        # Displaying only the first line of HoldReason, to bucket more efficiently
        reason = ad.eval("HoldReason").split('. ')[0]
        if "Error from" in reason and ": " in reason:
            parts = reason.split(": ", 1)
            if len(parts) == 2:
                reason = parts[1]

        reasons_by_code.setdefault(code, []).append((reason, subcode, proc_id, hold_time))

    print(f"Found {sum(len(v) for v in reasons_by_code.values())} held jobs\n", file=sys.stderr)
    return reasons_by_code


"""
Analyzes and prints time-based statistics for held jobs.

    Parameters:
        reasons_by_code (Dict): Dictionary grouping hold reasons by HoldReasonCode.
"""
def print_time_analysis(reasons_by_code):
    all_times = []
    for pairs in reasons_by_code.values():
        all_times.extend([hold_time for _, _, _, hold_time in pairs if hold_time > 0])
    
    if not all_times:
        print("⏱️  Time Analysis: No timestamp data available\n")
        return
    
    earliest = min(all_times)
    latest = max(all_times)
    current_time = time_module.time()
    
    print("⏱️  Time Analysis:")
    print(f"  First held: {datetime.datetime.fromtimestamp(earliest).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Last held:  {datetime.datetime.fromtimestamp(latest).strftime('%Y-%m-%d %H:%M:%S')}")
    duration_hours = (latest - earliest) / 3600
    print(f"  Duration:   {duration_hours:.1f} hours")
    
    # Calculate overall average hold time
    avg_hold_duration = (current_time - sum(all_times) / len(all_times))
    print(f"  Avg hold:   {format_duration(avg_hold_duration)}")
    
    print()


"""
Export job IDs with hold reason codes to a CSV file for bulk operations.

    Parameters:
        all_buckets (List): All buckets with job data.
        reasons_by_code (Dict): Dictionary mapping codes to job data.
        cluster_id (str): The cluster ID.
        filename (str): Output filename.
"""
def export_job_ids(all_buckets, reasons_by_code, cluster_id, filename):
    import csv
    
    # Build a mapping of proc_id to hold reason code
    proc_to_code = {}
    for code, pairs in reasons_by_code.items():
        for _, _, proc_id, _ in pairs:
            proc_to_code[proc_id] = code
    
    # Collect job IDs with their codes
    job_data = []
    seen_jobs = set()
    for bucket in all_buckets:
        for _, _, proc_id, _ in bucket:
            job_id = f"{cluster_id}.{proc_id}"
            if job_id not in seen_jobs:
                seen_jobs.add(job_id)
                hold_code = proc_to_code.get(proc_id, "Unknown")
                hold_label = HOLD_REASON_CODES.get(hold_code, {}).get("label", f"Code {hold_code}")
                job_data.append((job_id, hold_code, hold_label))
    
    # Sort by job ID for consistency
    job_data.sort(key=lambda x: (int(x[0].split('.')[0]), int(x[0].split('.')[1])))
    
    # Write to CSV
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["JobID", "HoldReasonCode", "HoldReasonLabel"])
        writer.writerows(job_data)
    
    print(f"✓ Exported {len(job_data)} unique job IDs to {filename}\n")

""" 
Processes grouped hold reasons and prints a detailed table with filtering and sorting options.

    Parameters:
        reasons_by_code (Dict): Dictionary grouping hold reasons by HoldReasonCode.
        cluster_id (str or int): The cluster ID being analyzed.
        args: Parsed command line arguments.
"""
def bucket_and_print_table(reasons_by_code, cluster_id, args):
    print(f"Cluster ID: {cluster_id}")

    held_jobs = sum(len(pairs) for pairs in reasons_by_code.values())
    print(f"Held Jobs in Cluster: {held_jobs}\n")
    
    # Time analysis
    print_time_analysis(reasons_by_code)

    example_rows = []
    all_buckets = []
    seen_codes = set()

    # Filter by specific code if requested
    if args.code:
        if args.code not in reasons_by_code:
            print(f"No held jobs found with HoldReasonCode {args.code}")
            return
        reasons_by_code = {args.code: reasons_by_code[args.code]}

    for code, pairs in reasons_by_code.items():
        label = HOLD_REASON_CODES.get(code, {}).get("label", f"Code {code}")
        seen_codes.add(code)
        buckets = bucket_reasons_with_data(pairs, threshold=args.threshold)
        
        for bucket in buckets:
            # Apply min-count filter
            if len(bucket) < args.min_count:
                continue
                
            all_buckets.append(bucket)
            example_reason, subcode, proc_id, hold_time = bucket[0]
            percent = (len(bucket) / held_jobs) * 100 if held_jobs > 0 else 0
            
            # Calculate average hold time for this bucket
            avg_hold_seconds, avg_hold_str = calculate_avg_hold_time(bucket)
            
            # Prepare job IDs string if requested
            job_ids_str = ""
            if args.show_job_ids:
                if len(bucket) <= 5:
                    ids = [str(p) for _, _, p, _ in bucket]
                    job_ids_str = ", ".join(ids)
                else:
                    ids = [str(p) for _, _, p, _ in bucket[:3]]
                    job_ids_str = f"{', '.join(ids)}... (+{len(bucket)-3} more)"
            
            row = [
                label, 
                subcode, 
                f"{percent:.1f}% ({len(bucket)})", 
                avg_hold_str,
                example_reason
            ]
            if args.show_job_ids:
                row.append(job_ids_str)
            
            # Store avg_hold_seconds for sorting
            row.append(avg_hold_seconds if avg_hold_seconds else 0)
            
            example_rows.append(row)

    # Sort results
    if args.sort_by == 'count':
        example_rows.sort(key=lambda x: int(x[2].split('(')[1].split(')')[0]), reverse=True)
    elif args.sort_by == 'code':
        example_rows.sort(key=lambda x: x[0])
    elif args.sort_by == 'percent':
        example_rows.sort(key=lambda x: float(x[2].split('%')[0]), reverse=True)
    elif args.sort_by == 'time':
        example_rows.sort(key=lambda x: x[-1], reverse=True)  # Sort by avg_hold_seconds
    
    # Remove the avg_hold_seconds column (used only for sorting)
    example_rows = [row[:-1] for row in example_rows]
    
    # Apply top N filter
    if args.top:
        example_rows = example_rows[:args.top]

    headers = ["Hold Reason Label", "SubCode", "% of Held Jobs (Count)", "Avg Hold Time", "Example Reason"]
    if args.show_job_ids:
        headers.append("Job IDs (ProcId)")
    
    print(tabulate(example_rows, headers=headers, tablefmt="grid"))

    print("\nLegend:")
    legend = []
    for code in sorted(seen_codes):
        entry = HOLD_REASON_CODES.get(code, {})
        legend.append([code, entry.get("label", "Unknown"), entry.get("reason", "No description available.")])
    print(tabulate(legend, headers=["Code", "Label", "Reason"], tablefmt="fancy_grid"))
    

    # Export job IDs if requested
    if args.export_jobs:
        export_job_ids(all_buckets, reasons_by_code, cluster_id, args.export_jobs)

def get_hold_bucket_data(cluster_id, threshold=0.7):
    """
    Return held jobs analysis data as a dictionary for use by cluster_health.py
    Does not print anything, just returns computed metrics.
    
    Returns:
        dict: Dictionary containing held jobs analysis
    """
    try:
        reasons_by_code = group_by_code(cluster_id)
        
        if not reasons_by_code:
            return {
                "held_count": 0,
                "held_codes": {},
                "held_reasons": {},
                "unique_reasons": 0,
                "buckets": [],
            }
        
        held_count = sum(len(pairs) for pairs in reasons_by_code.values())
        held_codes = {}
        all_buckets = []
        
        for code, pairs in reasons_by_code.items():
            held_codes[code] = len(pairs)
            buckets = bucket_reasons_with_data(pairs, threshold=threshold)
            all_buckets.extend(buckets)
        
        # Get top reasons
        held_reasons = {}
        for bucket in all_buckets:
            reason = bucket[0][0]  # Get example reason from first job
            held_reasons[reason] = len(bucket)
        
        # Time analysis
        all_times = []
        for pairs in reasons_by_code.values():
            all_times.extend([hold_time for _, _, _, hold_time in pairs if hold_time > 0])
        
        time_stats = {}
        if all_times:
            import time as time_module
            current_time = time_module.time()
            earliest = min(all_times)
            latest = max(all_times)
            avg_hold_duration = (current_time - sum(all_times) / len(all_times))
            
            time_stats = {
                "first_held": earliest,
                "last_held": latest,
                "duration_hours": (latest - earliest) / 3600,
                "avg_hold_duration": avg_hold_duration,
            }
        
        return {
            "held_count": held_count,
            "held_codes": held_codes,
            "held_reasons": held_reasons,
            "unique_reasons": len(held_reasons),
            "buckets": all_buckets,
            "time_stats": time_stats,
        }
    except Exception as e:
        return {
            "held_count": 0,
            "error": str(e)
        }



if __name__ == "__main__":
    args = parse_args()
    
    cluster_id = args.cluster_id
    reasons_by_code = group_by_code(cluster_id)
    
    if not reasons_by_code:
        print(f"No held jobs found in cluster {cluster_id}")
        sys.exit(0)
    
    bucket_and_print_table(reasons_by_code, cluster_id, args)

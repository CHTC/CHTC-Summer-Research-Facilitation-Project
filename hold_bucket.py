import sys
import htcondor2
from difflib import SequenceMatcher
from tabulate import tabulate


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


"""
Groups similar hold reason messages using fuzzy string matching (difflib.SequenceMatcher).
The default threshold has been kept as 0.7 as was found optimal by testing error messages

    Parameters:
        reason_list (List[str]): List of textual hold reasons for the jobs.
        subcodes (List[int]): Corresponding subcodes for the reasons.
        threshold (float): Similarity ratio (between 0 and 1) above which reasons are considered similar.

    Returns:
        List[List[Tuple[str, int]]]: A list of "buckets", where each bucket contains tuples of (reason, subcode)
                                     that are textually similar to each other.
"""

def bucket_reasons_with_subcodes(reason_list, subcodes, threshold=0.7):
    buckets = []
    for reason, subcode in zip(reason_list, subcodes):
        placed = False
        for bucket in buckets:
            ratio = SequenceMatcher(None, reason, bucket[0][0]).ratio()
            if ratio >= threshold:
                bucket.append((reason, subcode))
                placed = True
                break
        if not placed:
            buckets.append([(reason, subcode)])
    return buckets



""" 
Queries the HTCondor schedd for held jobs in the specified cluster and groups them by their HoldReasonCode.
The function then sends the groups of jobs with the same code to be bucketed by string similarity in bucket_reasons_with_subcodes()

    Parameters:
        cluster_id (str or int): The ID of the cluster to analyze.

    Returns:
        Dict[int, List[Tuple[str, int]]]: A dictionary mapping each HoldReasonCode to a list of (HoldReason, HoldReasonSubCode) tuples.
"""
def group_by_code(cluster_id):
    
    schedd = htcondor2.Schedd()
    reasons_by_code = {}

    for ad in schedd.query(
        constraint=f"ClusterId == {cluster_id} && JobStatus == 5",
        projection=["HoldReasonCode", "HoldReason", "HoldReasonSubCode"],
        limit=-1
    ):
        code = ad.eval("HoldReasonCode")
        subcode = ad.eval("HoldReasonSubCode")

        # Displaying only the first line of HoldReason, to bucket more efficiently
        reason = ad.eval("HoldReason").split('. ')[0]
        if "Error from" in reason and ": " in reason:
            parts = reason.split(": ", 1)
            if len(parts) == 2:
                reason = parts[1]

        reasons_by_code.setdefault(code, []).append((reason, subcode))

    return reasons_by_code





""" 
Processes grouped hold reasons and prints:
        - A table summarizing the percentage of jobs held for each bucketed reason.
        - A legend explaining each HoldReasonCode.
The function also take the HoldReason string and stores only the error message. 

NOTE: The information about the slots which sent the error which can be a future feature 

    Parameters:
        reasons_by_code (Dict[int, List[Tuple[str, int]]]): Dictionary grouping hold reasons by HoldReasonCode.
        cluster_id (str or int): The cluster ID being analyzed.

"""
def bucket_and_print_table(reasons_by_code, cluster_id):
    print()
    print("Cluster ID:", cluster_id)

    held_jobs = sum(len(pairs) for pairs in reasons_by_code.values())
    print("Held Jobs in Cluster:", held_jobs)

    example_rows = []
    seen_codes = set()

    for code, pairs in reasons_by_code.items():
        reasons, subcodes = zip(*pairs)
        label = HOLD_REASON_CODES.get(code, {}).get("label", f"Code {code}")
        seen_codes.add(code)
        buckets = bucket_reasons_with_subcodes(reasons, subcodes)
        for bucket in buckets:
            example_reason, subcode = bucket[0]
            percent = (len(bucket) / held_jobs) * 100 if held_jobs > 0 else 0
            example_rows.append([label, subcode, f"{percent:.1f}% ({len(bucket)})", example_reason])

    headers = ["Hold Reason Label", "SubCode", "% of Held Jobs (Count)", "Example Reason"]
    print(tabulate(example_rows, headers=headers, tablefmt="grid"))

    print("\nLegend:")
    legend = []
    for code in sorted(seen_codes):
        entry = HOLD_REASON_CODES.get(code, {})
        legend.append([code, entry.get("label", "Unknown"), entry.get("reason", "No description available.")])
    print(tabulate(legend, headers=["Code", "Label", "Reason"], tablefmt="fancy_grid"))





""" 
Main Execution Block:
    - Parses the cluster ID from command-line arguments.
    - Calls `group_by_code()` to gather held job reasons.
    - Passes the results to `bucket_and_print_table()` to display the report.

Example:
    $ python condor_hold_bucket.py 123456
"""
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python hold_bucket.py <ClusterId>")
        sys.exit(1)

    cluster_id = sys.argv[1]
    reasons_by_code = group_by_code(cluster_id)
    bucket_and_print_table(reasons_by_code, cluster_id)


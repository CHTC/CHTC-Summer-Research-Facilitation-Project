import argparse
import textwrap
import sys
import os


"""
Unified CLI entry point for HTCondor cluster analysis tools.

Usage:
    python main.py <cluster_id>                       # defaults to summarize
    python main.py <subcommand> <cluster_id>

Subcommands:
    dashboard   ASCII job-status bar chart
    histogram   Runtime distribution histogram + scatter plot
    analytics   Resource utilisation report (CPU / memory / disk)
    hold        Held-job classifier and bucketer
    summarize   Aggregated cluster health report (default)
"""


def ensure_cluster_data(cluster_id):
    """
    Check if cluster CSV data already exists. If not, fetch it automatically
    by calling fetch_cluster_data.fetch_cluster_jobs().

    Called before any subcommand that reads from cluster_data/.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if os.path.exists(filepath):
        return

    print(
        f"[info] No cached data found for cluster {cluster_id}.\n"
        f"[info] Fetching from HTCondor now — this may take a moment...\n"
    )

    try:
        from fetch_cluster_data import fetch_cluster_jobs, validate_cluster_exists
    except ImportError:
        print(
            "Error: fetch_cluster_data.py not found.\n"
            "Please make sure it is in the same directory as main.py."
        )
        sys.exit(1)

    if not validate_cluster_exists(cluster_id):
        print(
            f"Error: No jobs found for cluster {cluster_id}.\n"
            "Please verify:\n"
            "  1. The cluster ID is correct\n"
            "  2. You have permission to access this cluster\n"
            "  3. The cluster exists in HTCondor history or queue"
        )
        sys.exit(1)

    fetch_cluster_jobs(cluster_id, output_dir=data_dir)
    print()  # blank line before tool output


def build_parser():
    parser = argparse.ArgumentParser(
        prog="condor_profiler",
        description=textwrap.dedent(
            """
            HTCondor Cluster Profiler
            =========================
            A suite of tools for diagnosing and analysing HTCondor job clusters.

            Usage:
              python main.py <subcommand> <cluster_id>

            Subcommands:
              python main.py dashboard  12345
              python main.py histogram  12345
              python main.py analytics  12345
              python main.py hold       12345
              python main.py summarize  12345

            Run 'python main.py <subcommand> --help' for full options on each tool.
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    subparsers = parser.add_subparsers(
        dest="subcommand",
        metavar="<subcommand>",
    )

    # ------------------------------------------------------------------ #
    # dashboard                                                            #
    # ------------------------------------------------------------------ #
    dashboard_parser = subparsers.add_parser(
        "dashboard",
        help="ASCII job-status bar chart for a cluster",
        description=textwrap.dedent(
            """
            Display a real-time ASCII bar chart of job statuses.

            Queries both the HTCondor queue (live jobs) and history (completed
            jobs) and prints counts and percentages for every status:
            Idle, Running, Removing, Completed, Held, Transferring Output,
            Suspended.

            Example:
              python main.py dashboard 12345
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    dashboard_parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        help="HTCondor cluster ID to display.",
    )

    # ------------------------------------------------------------------ #
    # histogram                                                            #
    # ------------------------------------------------------------------ #
    histogram_parser = subparsers.add_parser(
        "histogram",
        help="Runtime distribution histogram and CDF",
        description=textwrap.dedent(
            """
            Plot job runtimes from cluster CSV data.

            Produces (by default, both):
              • A CDF line plot of runtimes
              • A percentile-binned histogram of runtimes

            Data files must be in: cluster_data/cluster_{id}_jobs.csv

            Examples:
              python main.py runtimes 12345
              python main.py runtimes 12345 --show cdf
              python main.py runtimes 12345 --show histogram
              python main.py runtimes 12345 --print-list
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    histogram_parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        help="HTCondor cluster ID to analyse.",
    )
    histogram_parser.add_argument(
        "--show",
        choices=["cdf", "histogram", "both"],
        default="both",
        help="Which graph to display: cdf, histogram, or both. (default: both)",
    )
    histogram_parser.add_argument(
        "--print-list",
        action="store_true",
        default=False,
        help="Print job IDs with runtime < 10 minutes. (default: off)",
    )
    histogram_parser.add_argument(
        "--percentiles",
        type=int,
        default=10,
        metavar="N",
        help="Number of percentile bins for the histogram. (default: 10)",
    )

    # ------------------------------------------------------------------ #
    # analytics                                                            #
    # ------------------------------------------------------------------ #
    analytics_parser = subparsers.add_parser(
        "resources",
        help="Resource utilisation report (CPU / memory / disk)",
        description=textwrap.dedent(
            """
            Analyse resource utilisation for completed HTCondor job clusters.

            Generates a report covering:
              • Requested vs. actual CPU, memory, and disk usage
              • Five-number summaries and efficiency percentages
              • Usage distribution histograms
              • Optimisation recommendations based on P95 patterns

            Savings are reported in GiB-hours, which captures both how much
            is wasted per job and how long jobs run. A figure like 1,200 GiB-hours
            is broken down inline as:
              (≈ 30.0 GiB/job × 40 jobs × 1.0 hr avg runtime)
            so you can sanity-check each factor independently.

            Data files must be in: cluster_data/cluster_{id}_jobs.csv

            Example:
              python main.py resources 12345
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    analytics_parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        help="HTCondor cluster ID to analyse.",
    )

    # ------------------------------------------------------------------ #
    # hold                                                                 #
    # ------------------------------------------------------------------ #
    hold_parser = subparsers.add_parser(
        "hold",
        help="Classify and bucket held jobs by hold reason",
        description=textwrap.dedent(
            """
            Analyse held jobs in an HTCondor cluster.

            Groups held jobs by HoldReasonCode, then uses fuzzy string
            matching to bucket jobs with similar hold reason messages.
            Prints a table with counts, percentages, average hold durations,
            and a human-readable legend of hold codes.

            Examples:
              python main.py hold 12345
              python main.py hold 12345 --min-count 10 --sort-by time
              python main.py hold 12345 --top 5 --code 34
              python main.py hold 12345 --export-jobs held.txt
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    hold_parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        help="HTCondor cluster ID to analyse.",
    )

    # Filtering
    hold_filter = hold_parser.add_argument_group("filtering options")
    hold_filter.add_argument(
        "--min-count",
        type=int,
        default=1,
        metavar="N",
        help="Only show buckets with at least N jobs. (default: 1)",
    )
    hold_filter.add_argument(
        "--top",
        type=int,
        metavar="N",
        help="Show only the top N most common buckets.",
    )
    hold_filter.add_argument(
        "--code",
        type=int,
        metavar="CODE",
        help="Filter to jobs with a specific HoldReasonCode.\n"
             "Common codes: 3 (JobPolicy), 34 (Memory), 12 (Output Transfer).",
    )

    # Sorting
    hold_sort = hold_parser.add_argument_group("sorting options")
    hold_sort.add_argument(
        "--sort-by",
        choices=["count", "code", "percent", "time"],
        default="count",
        help="Sort results by: count (default), code, percent, or time.",
    )

    # Bucketing
    hold_bucket = hold_parser.add_argument_group("bucketing options")
    hold_bucket.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        metavar="RATIO",
        help="Similarity threshold (0.0–1.0) for grouping similar error\n"
             "messages. Higher = stricter. (default: 0.7)",
    )

    # Output
    hold_output = hold_parser.add_argument_group("output options")
    hold_output.add_argument(
        "--show-job-ids",
        action="store_true",
        help="Display ProcIds in the output table.",
    )
    hold_output.add_argument(
        "--export-jobs",
        metavar="FILENAME",
        help="Export held job IDs to a CSV file for bulk operations.\n"
             "Format: one ClusterId.ProcId per line.",
    )

    # ------------------------------------------------------------------ #
    # summarize                                                            #
    # ------------------------------------------------------------------ #
    summarize_parser = subparsers.add_parser(
        "summarize",
        help="Aggregated cluster health report across all tools (default)",
        description=textwrap.dedent(
            """
            Generate a concise health report for a cluster.

            Aggregates data from analytics, dashboard, histogram, and
            hold_bucket to produce a single colour-coded status table with
            recommended next steps.

            This is the default subcommand — the following are equivalent:
              python main.py 12345
              python main.py summarize 12345
            """
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    summarize_parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        help="HTCondor cluster ID to summarize.",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Require a subcommand — print help if none given.
    if args.subcommand is None:
        parser.print_help()
        sys.exit(1)

    # Lazy imports so that missing optional deps (e.g. htcondor2) only
    # surface when you actually invoke the relevant subcommand.
    if args.subcommand == "dashboard":
        from dashboard import run
        run(args)

    elif args.subcommand == "histogram":
        ensure_cluster_data(args.cluster_id)
        from histogram import run
        run(args)

    elif args.subcommand == "analytics":
        ensure_cluster_data(args.cluster_id)
        from analytics import summarize
        summarize(args.cluster_id)

    elif args.subcommand == "hold":
        from hold_bucket import group_by_code, bucket_and_print_table
        reasons_by_code = group_by_code(args.cluster_id)
        if not reasons_by_code:
            print(f"No held jobs found in cluster {args.cluster_id}")
            sys.exit(0)
        bucket_and_print_table(reasons_by_code, args.cluster_id, args)

    elif args.subcommand == "summarize":
        ensure_cluster_data(args.cluster_id)
        from summarize import run
        run(args)


if __name__ == "__main__":
    main()
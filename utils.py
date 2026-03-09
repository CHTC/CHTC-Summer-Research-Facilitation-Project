import os
import csv
import sys
from datetime import datetime, timedelta


"""
Shared utility functions for the HTCondor cluster analytics suite.
"""


def safe_float(value):
    """
    Safely convert a value to float, returning None on failure.

    Parameters:
        value: Any value to convert.

    Returns:
        float or None
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def load_csv_for_cluster(cluster_id, exit_on_missing=True):
    """
    Load the cached CSV for a cluster and return a list of job dicts.

    Looks for: <script_dir>/cluster_data/cluster_<cluster_id>_jobs.csv

    Parameters:
        cluster_id (str or int): The cluster ID to load.
        exit_on_missing (bool): If True, print an error and sys.exit(1) when
            the file is not found. If False, return None instead.

    Returns:
        list[dict] or None
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        if exit_on_missing:
            print(
                f"Cluster data not found for cluster {cluster_id}. "
                "Please make sure you have the correct CSV file and cluster ID."
            )
            sys.exit(1)
        return None

    with open(filepath, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def format_seconds_human(seconds):
    """
    Format a duration in seconds as a compact human-readable string.

    Examples:
        45    -> "45s"
        130   -> "2m 10s"
        3700  -> "1h 1m 40s"
        90000 -> "1d 1h"

    Parameters:
        seconds (int or float): Duration in seconds.

    Returns:
        str
    """
    seconds = int(seconds)
    if seconds == 0:
        return "0s"
    parts = []
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def format_epoch_human_relative(epoch_seconds):
    """
    Format an epoch timestamp as a human-readable relative string.

    Examples:
        (30 seconds ago) -> "just now"
        (45 minutes ago) -> "45 minutes ago"
        (3 days ago)     -> "3 days ago"
        (older)          -> "2024-11-01"

    Parameters:
        epoch_seconds (int or float): Unix timestamp.

    Returns:
        str
    """
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
    except Exception:
        return "N/A"
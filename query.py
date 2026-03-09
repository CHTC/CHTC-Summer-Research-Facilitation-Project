import os
import csv
import sys
import argparse
import elasticsearch


"""
Elasticsearch Cluster Job Dumper

This script connects to the CHTC Elasticsearch server, fetches all jobs
for a given ClusterId (and optionally, a specific User), using the Scroll API
(to handle large result sets), and writes them into a CSV file inside the
'cluster_data/' directory.

Usage:
    python query.py <ClusterId> [--user USER] [--output-dir DIR]

NOTE: You need authentication to access data from the Elasticsearch database.
      Fill in ES_USER and ES_PASS before running.
"""

# Constants
ES_HOST = "https://elastic.osg.chtc.io/q"
ES_INDEX = "adstash-ospool-job-history-*"
MAX_RESULTS = 1000000
SCROLL_DURATION = "5m"

# Authentication — fill in before running
ES_USER = "*****"
ES_PASS = "************"


def connect_to_elasticsearch():
    es = elasticsearch.Elasticsearch(ES_HOST, http_auth=(ES_USER, ES_PASS))
    if not es.ping():
        print("Error: Failed to connect to Elasticsearch.")
        sys.exit(1)
    return es


def build_query(cluster_id, user=None):
    filters = [{"match": {"ClusterId": cluster_id}}]
    if user:
        filters.append({"match": {"Owner": user}})
    return {
        "query": {
            "bool": {
                "must": filters
            }
        }
    }


def parse_args():
    parser = argparse.ArgumentParser(
        prog="query",
        description="Fetch all jobs for a cluster from Elasticsearch and save to CSV.",
        epilog="Output: cluster_data/cluster_<CLUSTER_ID>_jobs.csv",
    )
    parser.add_argument(
        "cluster_id",
        metavar="CLUSTER_ID",
        type=int,
        help="HTCondor cluster ID to fetch (must be an integer).",
    )
    parser.add_argument(
        "--user",
        metavar="USER",
        default=None,
        help="Optional: filter jobs to a specific Owner/User.",
    )
    parser.add_argument(
        "--output-dir",
        metavar="DIR",
        default="cluster_data",
        help="Directory to save the CSV file (default: cluster_data/).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    cluster_id = args.cluster_id
    user = args.user
    output_dir = args.output_dir

    es = connect_to_elasticsearch()
    query = build_query(cluster_id, user)

    # Start scroll
    response = es.search(index=ES_INDEX, body=query, scroll=SCROLL_DURATION)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']

    all_hits = []
    fieldnames = set()

    while hits and len(all_hits) < MAX_RESULTS:
        remaining = MAX_RESULTS - len(all_hits)
        to_add = hits[:remaining]
        all_hits.extend(to_add)

        for hit in to_add:
            fieldnames.update(hit['_source'].keys())

        if len(all_hits) >= MAX_RESULTS:
            break

        response = es.scroll(scroll_id=scroll_id, scroll=SCROLL_DURATION)
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

    # Output directory and file
    # Always use cluster_<ID>_jobs.csv so the analytics suite can find it
    os.makedirs(output_dir, exist_ok=True)
    csv_filename = os.path.join(output_dir, f"cluster_{cluster_id}_jobs.csv")

    if user:
        print(f"Note: filtering by user '{user}' — output still written to standard filename for analytics suite compatibility.")
    print(f"📂 Writing to: {csv_filename}")

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()
        for hit in all_hits:
            writer.writerow(hit['_source'])

    print(
        f"Dumped {len(all_hits)} jobs for ClusterId {cluster_id}"
        + (f" filtered by user '{user}'" if user else "")
        + f" to {csv_filename}"
    )

    # Clean up scroll
    es.clear_scroll(scroll_id=scroll_id)


if __name__ == "__main__":
    main()
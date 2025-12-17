import os
import csv
import sys
import elasticsearch


"""
Elasticsearch Cluster Job Dumper

This script connects to the CHTC Elasticsearch server, fetches all jobs
for a given ClusterId (and optionally, a specific User), using the Scroll API
(to handle large result sets), and writes them into a CSV file inside the
'cluster_data/' directory.

Usage:
    query.py <ClusterId> [User]

NOTE: You need authentication to access data from the Elasticsearch database, that is why the ES_USER and ES_PASS are blanked 

"""

# Constants
ES_HOST = "https://elastic.osg.chtc.io/q"
ES_INDEX = "adstash-ospool-job-history-*"
MAX_RESULTS = 1000000
SCROLL_DURATION = "5m"

#Authenticaion to be filled
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

def main():
    if len(sys.argv) < 2:
        print("Usage: python dump_cluster_jobs.py <ClusterId> [User]")
        sys.exit(1)

    try:
        cluster_id = int(sys.argv[1])
    except ValueError:
        print("Error: ClusterId must be an integer.")
        sys.exit(1)

    user = sys.argv[2] if len(sys.argv) > 2 else None

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
    output_dir = os.path.join(os.getcwd(), "cluster_data")
    os.makedirs(output_dir, exist_ok=True)

    user_suffix = f"_{user}" if user else ""
    csv_filename = os.path.join(output_dir, f"cluster_{cluster_id}{user_suffix}_jobs.csv")

    print(f"📂 Writing to: {csv_filename}")

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()
        for hit in all_hits:
            writer.writerow(hit['_source'])

    print(f"Dumped {len(all_hits)} jobs for ClusterId {cluster_id}" + (f" and user '{user}'" if user else "") + f" to {csv_filename}")

    # Clean up scroll
    es.clear_scroll(scroll_id=scroll_id)

if __name__ == "__main__":
    main()

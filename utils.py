import htcondor2

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def fetch_schedd_data(clusterId):
    schedd = htcondor2.Schedd()
    counts = { state: 0 for state in job_states }
    # history (finished jobs)
    for ad in schedd.history(
            constraint = f"ClusterId == {clusterId}",
            projection = ["JobStatus"],
            match = -1
        ):
        counts[job_states[ad.eval("JobStatus")-1]] += 1
    # queue (running / pending jobs)
    for ad in schedd.query(
            constraint = f"ClusterId == {clusterId}",
            projection = ["JobStatus"],
            limit = -1
        ):
        counts[job_states[ad.eval("JobStatus")-1]] += 1
    return counts
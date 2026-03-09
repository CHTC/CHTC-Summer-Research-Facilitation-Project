import sys
from datetime import datetime
from tabulate import tabulate

try:
    from analytics import get_analytics_data
    from dashboard import get_dashboard_data
    from histogram import get_histogram_data
    from hold_bucket import get_hold_bucket_data
except ImportError as e:
    print(f"Error importing analysis modules: {e}")
    print("Make sure all analysis scripts are in the same directory.")
    sys.exit(1)

"""
This program provides a concise cluster health summary by aggregating
data from all analysis tools and providing tool recommendations.
"""

THRESHOLDS = {
    "memory_efficiency": {"critical": 15, "warning": 50, "good": 80},
    "disk_efficiency": {"critical": 15, "warning": 50, "good": 80},
    "cpu_efficiency": {"critical": 15, "warning": 50, "good": 80},
    "held_jobs_pct": {"critical": 20, "warning": 10},
    "fast_jobs_pct": {"critical": 30, "warning": 15},
    "runtime_variance": {"critical": 3.0, "warning": 2.0},
}


class Colors:
    RED = "\033[91m"
    YELLOW = "\033[93m"
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def color_text(text, color):
    return f"{color}{text}{Colors.RESET}"


def get_health_status(value, threshold_dict, higher_is_better=True):
    if higher_is_better:
        if value >= threshold_dict["good"]:
            return "HEALTHY"
        elif value >= threshold_dict["warning"]:
            return "WARNING"
        else:
            return "CRITICAL"
    else:
        if value >= threshold_dict["critical"]:
            return "CRITICAL"
        elif value >= threshold_dict["warning"]:
            return "WARNING"
        else:
            return "HEALTHY"


def get_status_symbol(status):
    if status == "CRITICAL":
        return color_text("🔴 CRITICAL", Colors.RED)
    elif status == "WARNING":
        return color_text("🟡 WARNING", Colors.YELLOW)
    else:
        return color_text("🟢 HEALTHY", Colors.GREEN)


def generate_health_report(cluster_id, efficiency_data, status_data, runtime_data, held_data):
    findings = []

    mem_eff = efficiency_data.get("memory_efficiency", 0)
    mem_status = get_health_status(mem_eff, THRESHOLDS["memory_efficiency"])
    mem_jobs = efficiency_data.get("memory_jobs", 0)
    if mem_status == "CRITICAL":
        reason = f"Severe over-provisioning ({mem_eff:.1f}% efficiency)"
    elif mem_status == "WARNING":
        reason = f"Low efficiency ({mem_eff:.1f}%)"
    else:
        reason = f"Well optimized ({mem_eff:.1f}% efficiency)"
    findings.append({
        "aspect": "Memory Efficiency", "status": mem_status,
        "value": f"{mem_eff:.1f}%", "jobs": mem_jobs, "reason": reason,
        "tool": f"python main.py analytics {cluster_id}",
    })

    disk_eff = efficiency_data.get("disk_efficiency", 0)
    disk_status = get_health_status(disk_eff, THRESHOLDS["disk_efficiency"])
    disk_jobs = efficiency_data.get("disk_jobs", 0)
    if disk_status == "CRITICAL":
        reason = f"Severe over-provisioning ({disk_eff:.1f}% efficiency)"
    elif disk_status == "WARNING":
        reason = f"Low efficiency ({disk_eff:.1f}%)"
    else:
        reason = f"Well optimized ({disk_eff:.1f}% efficiency)"
    findings.append({
        "aspect": "Disk Efficiency", "status": disk_status,
        "value": f"{disk_eff:.1f}%", "jobs": disk_jobs, "reason": reason,
        "tool": f"python main.py analytics {cluster_id}",
    })

    cpu_eff = efficiency_data.get("cpu_efficiency", 0)
    cpu_status = get_health_status(cpu_eff, THRESHOLDS["cpu_efficiency"])
    cpu_jobs = efficiency_data.get("cpu_jobs", 0)
    if cpu_status == "CRITICAL":
        reason = f"Severe over-provisioning ({cpu_eff:.1f}% efficiency)"
    elif cpu_status == "WARNING":
        reason = f"Low efficiency ({cpu_eff:.1f}%)"
    else:
        reason = f"Well optimized ({cpu_eff:.1f}% efficiency)"
    findings.append({
        "aspect": "CPU Efficiency", "status": cpu_status,
        "value": f"{cpu_eff:.1f}%", "jobs": cpu_jobs, "reason": reason,
        "tool": f"python main.py analytics {cluster_id}",
    })

    held_count = held_data.get("held_count", 0)
    held_pct = status_data.get("held_pct", 0)
    if held_count > 0:
        held_status = get_health_status(held_pct, THRESHOLDS["held_jobs_pct"], higher_is_better=False)
        unique_reasons = held_data.get("unique_reasons", 0)
        reason = (
            f"{held_count} jobs held ({held_pct:.1f}%), {unique_reasons} unique reasons"
            if held_status in ("CRITICAL", "WARNING")
            else f"{held_count} jobs held ({held_pct:.1f}%)"
        )
        findings.append({
            "aspect": "Held Jobs", "status": held_status,
            "value": str(held_count), "jobs": held_count, "reason": reason,
            "tool": f"python main.py hold {cluster_id}",
        })
    else:
        findings.append({
            "aspect": "Held Jobs", "status": "HEALTHY",
            "value": "0", "jobs": 0, "reason": "No held jobs", "tool": "N/A",
        })

    fast_jobs = runtime_data.get("fast_jobs", 0)
    fast_pct = runtime_data.get("fast_jobs_pct", 0)
    total_runtime_jobs = runtime_data.get("total_runtime_jobs", 0)
    if fast_jobs > 0:
        fast_status = get_health_status(fast_pct, THRESHOLDS["fast_jobs_pct"], higher_is_better=False)
        if fast_status == "CRITICAL":
            reason = f"{fast_jobs} jobs < 10min ({fast_pct:.1f}%), consider job bundling"
        elif fast_status == "WARNING":
            reason = f"{fast_jobs} jobs < 10min ({fast_pct:.1f}%), review job granularity"
        else:
            reason = f"{fast_jobs} jobs < 10min ({fast_pct:.1f}%)"
        findings.append({
            "aspect": "Fast Jobs", "status": fast_status,
            "value": f"{fast_pct:.1f}%", "jobs": fast_jobs, "reason": reason,
            "tool": f"python main.py histogram {cluster_id}",
        })
    else:
        findings.append({
            "aspect": "Fast Jobs", "status": "HEALTHY",
            "value": "0%", "jobs": 0, "reason": "No fast jobs detected", "tool": "N/A",
        })

    cv = runtime_data.get("cv", 0)
    correlation = runtime_data.get("correlation", 0)
    if cv > 0:
        cv_status = get_health_status(cv, THRESHOLDS["runtime_variance"], higher_is_better=False)
        if cv_status == "CRITICAL":
            reason = f"High variance (CV={cv:.2f}), inconsistent runtimes"
        elif cv_status == "WARNING":
            reason = f"Moderate variance (CV={cv:.2f})"
        else:
            reason = f"Consistent runtimes (CV={cv:.2f})"
        if correlation > 0.4:
            reason += ", later jobs slower"
        elif correlation < -0.4:
            reason += ", later jobs faster"
        findings.append({
            "aspect": "Runtime Consistency", "status": cv_status,
            "value": f"CV={cv:.2f}", "jobs": total_runtime_jobs, "reason": reason,
            "tool": f"python main.py histogram {cluster_id}",
        })
    else:
        findings.append({
            "aspect": "Runtime Consistency", "status": "HEALTHY",
            "value": "N/A", "jobs": 0, "reason": "No runtime data available", "tool": "N/A",
        })

    total_jobs = status_data.get("total_jobs", 0)
    completed = status_data.get("completed", 0)
    running = status_data.get("running", 0)
    idle = status_data.get("idle", 0)
    if total_jobs > 0:
        findings.append({
            "aspect": "Job Status", "status": "INFO",
            "value": str(total_jobs), "jobs": total_jobs,
            "reason": f"{completed} completed, {running} running, {idle} idle",
            "tool": f"python main.py dashboard {cluster_id}",
        })

    return findings


def print_health_summary(cluster_id, findings):
    print("\n" + "=" * 120)
    print(f"{color_text('HTCondor Cluster Health Report', Colors.BOLD + Colors.CYAN):^130}")
    print("=" * 120)
    print(f"Cluster ID: {cluster_id}")
    print(f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 120)

    critical_count = sum(1 for f in findings if f["status"] == "CRITICAL")
    warning_count = sum(1 for f in findings if f["status"] == "WARNING")

    if critical_count > 0:
        overall = color_text("🔴 CRITICAL", Colors.RED)
    elif warning_count > 0:
        overall = color_text("🟡 WARNING", Colors.YELLOW)
    else:
        overall = color_text("🟢 HEALTHY", Colors.GREEN)

    print(f"\nOverall Status: {overall}")
    print(f"Critical Issues: {critical_count} | Warnings: {warning_count}")
    print()

    table_data = []
    for finding in findings:
        if finding["status"] == "INFO":
            status_display = color_text("ℹ️  INFO", Colors.CYAN)
        else:
            status_display = get_status_symbol(finding["status"])
        table_data.append([
            finding["aspect"], status_display, finding["value"],
            finding["reason"], finding["tool"],
        ])

    headers = ["Aspect", "Status", "Value", "Reason / Details", "Tool for Details"]
    print(tabulate(table_data, headers=headers, tablefmt="grid", maxcolwidths=[20, 15, 12, 50, 35]))

    print("\n" + "=" * 120)
    print(f"{color_text('Recommended Next Steps', Colors.BOLD)}")
    print("=" * 120)

    if critical_count > 0:
        print(f"\n{color_text('🔴 HIGH PRIORITY:', Colors.RED)}")
        for finding in findings:
            if finding["status"] == "CRITICAL" and finding["tool"] != "N/A":
                print(f"  • {finding['aspect']}: {finding['tool']}")

    if warning_count > 0:
        print(f"\n{color_text('🟡 REVIEW:', Colors.YELLOW)}")
        for finding in findings:
            if finding["status"] == "WARNING" and finding["tool"] != "N/A":
                print(f"  • {finding['aspect']}: {finding['tool']}")

    if critical_count == 0 and warning_count == 0:
        print(f"\n{color_text('✓ Cluster is healthy - no immediate action required', Colors.GREEN)}")
        print("  • Continue regular monitoring")
        print("  • Run weekly health checks")

    print("\n" + "=" * 120)
    print()


def run(args):
    """Entry point called by main.py."""
    cluster_id = args.cluster_id

    efficiency_data = get_analytics_data(cluster_id)
    if not efficiency_data:
        print(f"ERROR: Could not load analytics data.")
        print(f"Please run: python main.py fetch {cluster_id}")
        sys.exit(1)

    status_data = get_dashboard_data(cluster_id)
    if not status_data:
        status_data = {
            "total_jobs": efficiency_data.get("total_jobs", 0),
            "status_counts": {}, "held": 0, "held_pct": 0,
            "completed": 0, "running": 0, "idle": 0,
        }

    runtime_data = get_histogram_data(cluster_id)
    if not runtime_data:
        runtime_data = {
            "cv": 0, "fast_jobs": 0, "fast_jobs_pct": 0,
            "total_runtime_jobs": 0, "correlation": 0,
        }

    held_data = get_hold_bucket_data(cluster_id)
    if not held_data:
        held_data = {"held_count": 0, "unique_reasons": 0}

    findings = generate_health_report(
        cluster_id, efficiency_data, status_data, runtime_data, held_data
    )
    print_health_summary(cluster_id, findings)
# CHTC Summer Research Facilitation Project

**Fellow**: Kashika Mahajan  
**Mentors**: Andrew Owen, Ian Ross  
**Fellowship Dates**: May 19 – August 8, 2025

________


## 📚 Background

Researchers using HTCondor often struggle to quickly understand how their computational workloads (clusters of jobs) are performing. Current interfaces expose too much raw data, making it difficult, especially for less experienced users—to diagnose issues like  jobs on hold, poor resource utilization, or unexpected failures.

This project aimed to build tools that simplify job monitoring, flag issues, and offer meaningful insights into workload behavior using accessible metrics and clear visual feedback.

________


## 📁 Repository Structure

  - hold_classifer.py # Diagnoses held jobs and groups by hold reasons
  - runtime_histogram.py # Plots job runtime distribution using ASCII histograms
  - resource_usage_summary.py # Summarizes requested vs actual CPU, memory, disk
  - cluster_status_dashboard.py # Prints status distribution of jobs in a cluster
  - README.md # This file

________


## ⚙️ Setup and Installation
1. Clone this repository:
2. Install all the packages in the requirements.txt
3. You must have access to:
  - HTCondor Python bindings
  - Elasticsearch (if querying historical job data)
  ⚠️ Note: Some tools require authentication to the CHTC Elasticsearch instance, which is currently not available to general users.

________

## 🚀 Usage Instructions

Each tool is meant to be run as a standalone Python script with a cluster ID as input.

Example command: `python dashboard.py <ClusterId>`

________


## Features and Deliverables
1. Cluster Status Dashboard
    Purpose: Quickly visualize job statuses (Idle, Running, Held, Completed)
    Features:
      - Combines data from both queue and history
      - Highlights abnormal patterns using ASCII charts
  
### Example: Cluster Status Dashboard Output
```
Cluster 12345 Status Dashboard

             Status | Bar                                                | Count |      %
-----------------------------------------------------------------------------------------
               Idle | █████████████                                      |  8686 |  27.0%
            Running |                                                    |   433 |   1.3%
           Removing |                                                    |     0 |   0.0%
          Completed |                                                    |     0 |   0.0%
               Held | ███████████████████████████████████                | 23067 |  71.7%
Transferring Output |                                                    |     0 |   0.0%
          Suspended |                                                    |     0 |   0.0%

```

  
3. Cluster Runtime Histogram
    Purpose: Understand runtime variance across jobs
    Features:
      - Binned by percentiles
      - Flags jobs with runtime < 10 min
      - Can print list of affected job IDs
  
 
### Example: Cluster Runtime Histogram Output

<img width="500" height="300" alt="image" src="https://github.com/user-attachments/assets/d6102c28-8a1b-4d7e-b87b-2b0d6be26019" />



4. Hold Classifier
    Purpose: Explain why jobs were held
    Features:
      - Clusters jobs by HoldReasonCode + HoldReasonSubCode
      - Displays percentage and example reasons
      - Includes human-readable legend of hold codes
  
### Example: Hold Classifier Output

``` 

Cluster ID: 12345
Held Jobs in Cluster: 109
+---------------------+-----------+--------------------------+---------------------------------------------------------+
| Hold Reason Label   |   SubCode | % of Held Jobs (Count)   | Example Reason                                          |
+=====================+===========+==========================+=========================================================+
| StartdHeldJob       |         0 | 95.4% (104)              | Job failed to complete in 72 hrs                        |
+---------------------+-----------+--------------------------+---------------------------------------------------------+
| JobExecuteExceeded  |         0 | 4.6% (5)                 | The job exceeded allowed execute duration of 3+00:00:00 |
+---------------------+-----------+--------------------------+---------------------------------------------------------+

Legend:
╒════════╤════════════════════╤═══════════════════════════════════════════════════════════════════════════╕
│   Code │ Label              │ Reason                                                                    │
╞════════╪════════════════════╪═══════════════════════════════════════════════════════════════════════════╡
│     21 │ StartdHeldJob      │ The job was put on hold because WANT_HOLD in the machine policy was true. │
├────────┼────────────────────┼───────────────────────────────────────────────────────────────────────────┤
│     47 │ JobExecuteExceeded │ The job's allowed execution time was exceeded.                            │
╘════════╧════════════════════╧═══════════════════════════════════════════════════════════════════════════╛

```


5. Resource Utilization Report
    Purpose: Compare requested vs actual usage
    Features:
      - Summarizes CPU, memory, and disk usage
      - Adds flags for under (<15%) or over (>80%) utilization
      - Includes bar chart and percentiles


### Example: Resource Utilization ReportOutput  
```
================================================================================
                       HTCondor Cluster Resource Summary                        
================================================================================
          Cluster ID: 12345
           Job Count: 748
         Avg Runtime: 0:56:52

                              Requested Resources                               
================================================================================
Memory (GiB)   :
                 0.49       GiB    1 job(s)
                 12.0       GiB    1 job(s)
                 50.0       GiB    746 job(s)

Disk (GiB)     :
                 0.1        GiB    1 job(s)
                 10.0       GiB    1 job(s)
                 30.0       GiB    746 job(s)

CPUs           :
                 1                 2 job(s)
                 8                 746 job(s)

GPUs           : No data
                              Number Summary Table                              
================================================================================
Resource (units)         :    Min      Q1   Median      Q3     Max   StdDev
--------------------------------------------------------------------------------
Memory Used (GiB)        :    0.1     1.2      6.1    14.2    47.4     10.4
Disk Used (GiB)          :    0.0     0.8      0.8     0.8     1.1      0.1
CPU Usage (%)            :  0.0%%  32.1%%   35.8%%  44.8%%  85.5%%   11.0%%

                              Overall Utilization                               
================================================================================
  Memory usage      [██████                                            ] 12.2%
  Disk usage        [█                                                 ] 2.6%
  CPU usage         [█████████████████                                 ] 35.8%

                                Efficiency Notes                                
================================================================================
  ⚠️  Memory usage is 12.2%
  ⚠️  Disk usage is 2.6%
  ✅ CPU usage is 35.8%

                                 End of Summary                                 
================================================================================

```



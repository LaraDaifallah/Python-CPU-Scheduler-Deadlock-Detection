# Python CPU Scheduler and Deadlock Detection

A discrete-time operating systems simulator for CPU scheduling, resource allocation, and deadlock detection and recovery.

## Features

- Preemptive priority scheduling: priorities range from 0 (highest) to 20 (lowest).
- Round Robin among ready processes at the same priority, with an 8-tick quantum.
- Aging: a ready process improves its priority by one after 10 waiting ticks.
- CPU execution and overlapping process I/O waits.
- Resource requests, releases, and blocking when resources are unavailable.
- Deadlock detection using available resources, allocations, and pending requests.
- Recovery by terminating a detected process with the lowest current priority; ties select the highest PID.
- Event logs, a text Gantt timeline, average ready waiting time, average turnaround time, and a deadlock report.

Priorities reset to their base values after I/O. Resources still held by a terminating process are released automatically.

## Files

| File | Purpose |
| --- | --- |
| scheduler.py | Input parser, process model, scheduler, resource management, and final reports. |
| innn.txt | Supplied five-process workload with two resource types. |

## Run

Requires Python 3.7 or newer. Uses only the Python standard library.

```sh
python scheduler.py
```

Run from the repository root so the simulator can find `innn.txt`. Edit that file to change the workload.

## Input format

The first line declares resources as `[resource_id, total_instances]`.

Each remaining line has:

```text
PID ARRIVAL_TIME PRIORITY CPU{commands} IO{duration} CPU{commands} ...
```

Inside a CPU block, an integer is a CPU duration, `R[id,count]` requests resource instances, and `F[id,count]` releases them.

For example:

```text
[1,10], [2,10]
1 0 2 CPU{10, R[1,1], 5, F[1,1]} IO{2} CPU{8}
```

This process arrives at time 0 with priority 2, runs for 10 ticks, requests one instance of resource 1, runs for 5 more ticks, releases it, waits for I/O, and resumes CPU execution.

Use unique PIDs, declared resource IDs, priorities from 0 to 20, positive CPU durations, and valid resource releases. The parser does not fully validate these constraints.

## Reports and interpretation

Waiting time counts time in READY queues; it excludes I/O and resource-blocked time. Turnaround time is finish time minus arrival time. Averages include processes terminated during recovery.

The Gantt timeline lists each change of CPU owner and ends with the number of recorded ticks. This is text output rather than a graphical chart.

The supplied workload requests only one instance from resource pools of ten, with matching releases; it is intended as a no-deadlock example. Deadlock screenshots may represent other workloads and are not proof of this input's output.

## Limits

The simulator stops after 100,000 ticks. In the current implementation this cutoff is silent, and unfinished processes contribute zero turnaround time, so final averages at that limit should not be treated as completed-run statistics.

Source was assembled from the supplied code parts with formatting and documentation cleanup. Execution has not been verified in this environment.

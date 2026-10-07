# GreenDIRAC

GreenDIRAC is a Python extension for DIRAC, developed for the GreenDIGIT
project. It adds environmental reporting and green-aware queue ordering to
DIRAC workload management. Completed jobs are enriched with energy, carbon,
PUE, and computing-efficiency metrics; those measurements are then reused to
prefer greener queues for later workloads.

## Architecture

```text
JobDB + ElasticJobParametersDB
              |
              v
     GreenReportingAgent <------ CIM/KPI services
          |          |
          |          +---------> CIM metrics endpoint
          v
Dedicated rolling Elasticsearch indexes
          |
          v
      SiteDirector <------------ CIM/KPI services
          |
          v
Queues sorted by descending GreenScore
```

The reporting and scheduling paths share Elasticsearch through the
`GreenMetricsIndexBase` option. The reporting agent writes the measurements;
the SiteDirector reads their recent CEE values.

## Components

### GreenReportingAgent

`GreenReportingAgent` selects completed or failed jobs that have not yet been
processed, then:

1. reads job parameters from `ElasticJobParametersDB` and attributes from
   `JobDB`;
2. resolves the DIRAC site to its GOCDB name;
3. retrieves PUE and carbon intensity for the execution interval through
   `CIMClient`;
4. computes energy, carbon footprint, CEE, efficiency, and work-per-energy;
5. submits the enriched record to the CIM metrics endpoint;
6. stores the record in a dedicated rolling Elasticsearch index;
7. marks the job as processed only after both writes succeed.

The current energy model uses an idle-consumption factor of `0.4`:

```text
effective_time = (1 - 0.4) * CPUTime + 0.4 * WallClockTime
energy_kWh = effective_time * (cores_used / total_cores) * TDP / 3,600,000
carbon_g = energy_kWh * PUE * carbon_intensity_g_per_kWh
CEE = CPUNormalizationFactor * total_cores / TDP
```

Failed jobs without an `EndExecTime` are skipped because an execution interval
cannot be constructed for them.

### Green-aware SiteDirector

The custom `SiteDirector` keeps the standard DIRAC submission flow and
overrides queue ordering. It:

1. reads the average CEE per `GridCE` from the last 30 days of green-metrics
   indexes;
2. retrieves PUE and carbon intensity once per distinct site;
3. computes `GreenScore = CEE / (PUE * CI)` for each queue;
4. sorts queues by descending GreenScore;
5. falls back to the standard DIRAC queue ordering if green-metric calculation
   fails.

The current implementation also forces the pilot option `--MaxCycles 50`.

### CIMClient

`CIMClient` provides the shared interface to the CIM and KPI services. It
handles authentication, DIRAC-to-GOCDB site resolution, request retries,
timeouts, token reuse, time-bucketed PUE/CI caching, stale-value fallback, and
metric submission.

## Elasticsearch writer/reader contract

Both agents default to the same index base:

```text
GreenMetricsIndexBase = dirac-egi-_greenmetrics_index
```

`GreenReportingAgent` creates rolling indexes such as:

```text
dirac-egi-_greenmetrics_index_0.0m
dirac-egi-_greenmetrics_index_1.0m
```

Each index accepts up to `GreenMetricsMaxDocuments` records, which defaults to
`1,000,000`. `SiteDirector` derives the matching read pattern from the same
base:

```text
dirac-egi-_greenmetrics_index_*
```

The relevant agent options are:

| Agent | Option | Default |
| --- | --- | --- |
| `GreenReportingAgent` | `GreenMetricsIndexBase` | `dirac-egi-_greenmetrics_index` |
| `GreenReportingAgent` | `GreenMetricsMaxDocuments` | `1000000` |
| `GreenReportingAgent` | `MaxJobsAtOnce` | `1000` |
| `SiteDirector` | `GreenMetricsIndexBase` | `dirac-egi-_greenmetrics_index` |

If the index base is overridden, configure the same value in both agent
sections. Elasticsearch job parameters must also be enabled:

```text
/Services/JobMonitoring/useESForJobParametersFlag = true
```

`GreenReportingAgent` refuses to initialize without
`ElasticJobParametersDB` because it uses that service both as its parameter
source and as its green-metrics destination.

## CIM configuration

`CIMClient` reads its configuration from:

```text
src/GreenDIRAC/WorkloadManagementSystem/Client/cim.conf
```

The supported sections are:

- `[CIM]`: `EMAIL`, `PASSWORD`, `API_BASE`, and `METRICS_URL`;
- `[KPI]`: `API_BASE`;
- `[Defaults]`: `PUE`, `CI`, and `ENERGY_WH`;
- `[Runtime]`: `TOKEN_MAX_AGE_H`, `CACHE_TTL`, `TOKEN_TIMEOUT_S`,
  `PUE_TIMEOUT_S`, `CI_TIMEOUT_S`, `SUBMIT_TIMEOUT_S`, plus optional cache and
  retry settings.

If the file is absent, client construction fails with
`RuntimeError("CIMClient config file not found: ...")`. Do not commit live
credentials or tokens to the repository.

## Research trace simulator

The independent trace-driven simulator is included as a Git submodule at:

```text
research/DIRACGreenScoreTraceSimulator
```

It compares randomized and GreenScore-based placement over historical DIRAC
job traces, including carbon and water-scarcity accounting. It is intentionally
isolated from `src/GreenDIRAC`: production agents do not import simulator code,
and installing the GreenDIRAC package does not install the simulator.

See
[`research/DIRACGreenScoreTraceSimulator/README.md`](research/DIRACGreenScoreTraceSimulator/README.md)
for its inputs, environment variables, test commands, and experiment launcher.

Initialize it after cloning with:

```bash
git submodule update --init --recursive
```

## Installation

GreenDIRAC requires Python 3.11 or newer and DIRAC 8.0.78-compatible releases.
For a development installation:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .
```

The package registers the `GreenDIRAC` extension through the `dirac` entry-point
group. Extension metadata, including the supported DIRAC setups, is defined in
`src/GreenDIRAC/__init__.py`.

## Repository layout

```text
src/GreenDIRAC/
├── __init__.py
└── WorkloadManagementSystem/
    ├── Agent/
    │   ├── GreenReportingAgent.py
    │   └── SiteDirector.py
    └── Client/
        └── CIMClient.py

research/
└── DIRACGreenScoreTraceSimulator/   # independent Git submodule
```

## Operational checks

Before deploying a change:

1. confirm that `GreenMetricsIndexBase` is identical for the reporting agent
   and SiteDirector;
2. confirm that Elasticsearch job parameters are enabled and writable;
3. verify that the CIM/KPI endpoints and credentials are valid;
4. check the SiteDirector logs for non-zero Elasticsearch hits and CEE buckets;
5. verify that jobs are marked processed only after both CIM and Elasticsearch
   storage succeed.

# GreenDIRAC

GreenDIRAC is a DIRAC extension built for the GreenDIGIT Project. It enriches
DIRAC workload management with sustainability data—power usage effectiveness
(PUE) and carbon intensity (CI)—so scheduling and reporting tools can account
for the environmental cost of running jobs.

## What GreenDIRAC adds
- Registers itself as a DIRAC extension so the agents and configuration entries
  are available alongside standard DIRAC deployments.
- Uses the collected metrics to prefer greener queues when dispatching jobs and
  to record energy/emissions for completed workloads.

## Core agents
- **Green-aware Site Director** – augments queue information with PUE, CI and
  then computes a green score that ranks queues from greenest to least green
  before scheduling.
- **GreenReportingAgent** – collects finished job records, queries CIM/CI
  services for sustainability data, computes energy/emissions figures, and
  persists the metrics to DIRAC databases and ElasticSearch when enabled.

## Local CIM configuration
`CIMClient` reads credentials and API endpoints from:
`src/GreenDIRAC/WorkloadManagementSystem/Client/cim.conf`

Required sections and keys:
- `[CIM]`: `EMAIL`, `PASSWORD`, `API_BASE`, `METRICS_URL`
- `[KPI]`: `API_BASE`
- `[Defaults]`: `PUE`, `CI`, `ENERGY_WH`
- `[Runtime]`: `TOKEN_MAX_AGE_H`, `CACHE_TTL`, `TOKEN_TIMEOUT_S`,
  `PUE_TIMEOUT_S`, `CI_TIMEOUT_S`, `SUBMIT_TIMEOUT_S`

If `cim.conf` is missing, `CIMClient` fails at startup with
`RuntimeError("CIMClient config file not found: ...")`.

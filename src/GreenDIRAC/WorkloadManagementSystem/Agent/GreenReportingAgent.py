#!/usr/bin/env python3
"""
GreenReportingAgent — Reads job parameters from ElasticJobParametersDB,
computes and submits green metrics, and stores final records in dedicated
rolling Elasticsearch indexes.
"""

from datetime import timezone
import re
import time

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client import PathFinder

from GreenDIRAC.WorkloadManagementSystem.Client.CIMClient import CIMClient

# --------------------------------------------------------
# DIRAC job parameters and attributes
# --------------------------------------------------------
JOB_PARAMETER_KEYS = [
    "ModelName", "CPUNormalizationFactor", "HostName", "JobID", "JobType",
    "LoadAverage", "MemoryUsed(kb)", "NormCPUTime(s)", "ScaledCPUTime(s)",
     "TotalCPUTime(s)", "WallClockTime(s)", "DiskSpace(MB)",
    "CEQueue", "GridCE",
]

JOB_ATTRIBUTE_KEYS = [
    "JobGroup", "JobName", "Owner", "OwnerDN", "OwnerGroup",
    "RescheduleCounter", "Site",
    "Status",
    "SubmissionTime", "StartExecTime", "EndExecTime",
    "SystemPriority", "UserPriority",
]

SITES_EUROPE = [
    "Cloud.IHPC.fr",
    "EGI.ARNES.si",
    "EGI.AUVERGRID.fr",
    "EGI.BARI.it",
    "EGI.CATANIA.it",
    "EGI.CERN.ch",
    "EGI.CESNET.cz",
    "EGI.CIEMAT.es",
    "EGI.CIRMMP.it",
    "EGI.CNAF.it",
    "EGI.CNR.it",
    "EGI.CPPM.fr",
    "EGI.CREATIS.fr",
    "EGI.CYFRONET.pl",
    "EGI.DESY.de",
    "EGI.DESYZN.de",
    "EGI.FRASCATI.it",
    "EGI.GOEGRID.de",
    "EGI.GRIDKA.de",
    "EGI.GRIF.fr",
    "EGI.HEPACC.uk",
    "EGI.IFAE.es",
    "EGI.IFCA.es",
    "EGI.IN2P3-CC.fr",
    "EGI.INFN-COSENZA.it",
    "EGI.INFN-GENOVA.it",
    "EGI.INFN-LECCE.it",
    "EGI.INFN-NAPOLI.it",
    "EGI.INFN-PISA.it",
    "EGI.INGRID.pt",
    "EGI.IRB.hr",
    "EGI.IRES.fr",
    "EGI.JINR.ru",
    "EGI.KFKI.hu",
    "EGI.LAPP.fr",
    "EGI.LNL.it",
    "EGI.LPC.fr",
    "EGI.LSGRUG.nl",
    "EGI.METU.tr",
    "EGI.NCBJ.pl",
    "EGI.NIKHEF.nl",
    "EGI.ROMA3.it",
    "EGI.RWTH-Aachen.de",
    "EGI.SARA.nl",
    "EGI.SRCE.hr",
    "EGI.SiGNET.si",
    "EGI.TASK.pl",
    "EGI.TORINO.it",
    "EGI.TRIESTE.it",
    "EGI.UCL.be",
    "EGI.UKI.uk",
    "EGI.UKIAC.uk",
    "EGI.UKIB.uk",
    "EGI.UKID.uk",
    "EGI.UKIG.uk",
    "EGI.UKIL.uk",
    "EGI.UKILH.uk",
    "EGI.UKIM.uk",
    "EGI.UKIMBH.uk",
    "EGI.UKIR.uk",
    "EGI.UKIRALPP.uk",
    "EGI.RAL.uk",
    "EGI.UKISHEF.uk",
    "EGI.ULAKBIM.tr",
    "EGI.ULB.be",
    "EGI.UNI-SIEGEN-HEP.de",
]

TIME_STAMPS = ["SubmissionTime", "StartExecTime", "EndExecTime"]

DEFAULT_TDP = 150

IDLE_CONSUMPTION_FACTOR = 0.4

GREEN_METRICS_MAPPING = {
    "properties": {
        "AgentLocalSE": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "BatchSystem": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "CEE": {"type": "float"},
        "CEQueue": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "CFP_g": {"type": "float"},
        "CI_g": {"type": "float"},
        "CPUNormalizationFactor": {"type": "long"},
        "DiskSpace(MB)": {"type": "float"},
        "Efficiency": {"type": "float"},
        "EndExecTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time",
        },
        "Energy_wh": {"type": "float"},
        "Error Message": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "ErrorMessage": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "ExecUnitFinished": {"type": "long"},
        "ExecUnitID": {"type": "long"},
        "GridCE": {"type": "keyword"},
        "HostName": {"type": "keyword"},
        "JobGroup": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "JobID": {"type": "long"},
        "JobName": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "JobType": {"type": "keyword"},
        "JobWrapperPID": {"type": "long"},
        "LastUpdateCPU(s)": {"type": "float"},
        "LoadAverage": {"type": "float"},
        "LocalAccount": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "LocalJobID": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "MatcherServiceTime": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "Memory(MB)": {"type": "long"},
        "Memory(kB)": {"type": "long"},
        "MemoryUsed(MB)": {"type": "float"},
        "MemoryUsed(kb)": {"type": "long"},
        "ModelName": {"type": "keyword"},
        "NCores": {"type": "long"},
        "NormCPUTime(s)": {"type": "long"},
        "OutputData": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "OutputSandbox": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "OutputSandboxLFN": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "OutputSandboxMissingFiles": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "Owner": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "OwnerDN": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "OwnerGroup": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "PUE": {"type": "float"},
        "PayloadPID": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "PendingRequest": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "PilotAgent": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "Pilot_Reference": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "RescheduleCounter": {"type": "long"},
        "ScaledCPUTime(s)": {"type": "float"},
        "Site": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "SiteDIRAC": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "SiteGOCDB": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "StandardOutput": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "StartExecTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time",
        },
        "Status": {"type": "keyword"},
        "SubmissionTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time",
        },
        "SystemPriority": {"type": "long"},
        "TDP_w": {"type": "long"},
        "TotalCPUTime(s)": {"type": "long"},
        "UserPriority": {"type": "long"},
        "WallClockTime(s)": {"type": "float"},
        "Work": {"type": "float"},
        "timestamp": {"type": "date"},
    }
}


# ==========================================================
#               GreenReportingAgent
# ==========================================================
class GreenReportingAgent(AgentModule):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.jobDB = None
        self.elasticJobParametersDB = None
        self.maxJobsAtOnce = 1000
        self.greenMetricsIndexBase = "dirac-egi-_greenmetrics_index"
        self.greenMetricsMaxDocuments = 1_000_000
        self._activeGreenMetricsIndex = None
        self._activeGreenMetricsCount = 0
        self._ensuredGreenMetricsIndexes = set()

        self.section = PathFinder.getAgentSection(self.agentName)

        # CIM abstraction
        self.cimClient = None

    # -----------------------------------------------------
    def initialize(self):

        self.jobDB = JobDB()

        # ElasticSearch support
        self.elasticJobParametersDB = None
        useES = Operations().getValue(
            "/Services/JobMonitoring/useESForJobParametersFlag", False
        )

        if useES:
            res = ObjectLoader().loadObject(
                "WorkloadManagementSystem.DB.ElasticJobParametersDB",
                "ElasticJobParametersDB",
            )
            if res["OK"]:
                self.elasticJobParametersDB = res["Value"](parentLogger=self.log)
                self.log.info("Using ElasticJobParametersDB")
            else:
                self.log.warn("Falling back to JobDB for job parameters")

        self.maxJobsAtOnce = self.am_getOption(
            "MaxJobsAtOnce", self.maxJobsAtOnce
        )

        if not self.elasticJobParametersDB:
            return S_ERROR(
                "GreenReportingAgent requires ElasticJobParametersDB; "
                "enable /Services/JobMonitoring/useESForJobParametersFlag"
            )

        self.greenMetricsIndexBase = self.am_getOption(
            "GreenMetricsIndexBase", self.greenMetricsIndexBase
        ).lower()
        self.greenMetricsMaxDocuments = int(
            self.am_getOption(
                "GreenMetricsMaxDocuments", self.greenMetricsMaxDocuments
            )
        )
        if self.greenMetricsMaxDocuments < 1:
            return S_ERROR("GreenMetricsMaxDocuments must be greater than zero")

        result = self.__initializeActiveGreenMetricsIndex()
        if not result["OK"]:
            return result

        # Instantiate CIM client
        self.cimClient = CIMClient(logger=self.log)

        # Load CPU models
        self.cpuDict = {}
        res = gConfig.getSections(f"{self.section}/CPUData")
        if res["OK"]:
            for model in res["Value"]:
                self.cpuDict[model] = {
                    "TDP": gConfig.getValue(
                        f"{self.section}/CPUData/{model}/TDP", DEFAULT_TDP
                    ),
                    "Cores": gConfig.getValue(
                        f"{self.section}/CPUData/{model}/Cores", 12
                    ),
                }

        self.log.info(f"Loaded {len(self.cpuDict)} CPU models")
        return S_OK()

    # =====================================================================
    # EXECUTE
    # =====================================================================
    def execute(self):
        condDict = {
            "Status": [JobStatus.DONE, JobStatus.FAILED],
            "Site": SITES_EUROPE,
            "ApplicationNumStatus": 0,
        }

        res = self.jobDB.selectJobs(
            condDict,
            limit=self.maxJobsAtOnce,
            orderAttribute="LastUpdateTime:DESC",
        )
        if not res["OK"]:
            return res

        jobIDs = [int(j) for j in res["Value"]]
        if not jobIDs:
            return S_OK()

        # Load job parameters
        if self.elasticJobParametersDB:
            params = self.elasticJobParametersDB.getJobParameters(jobIDs)
        else:
            params = self.jobDB.getJobParameters(jobIDs)

        attrs = self.jobDB.getJobsAttributes(jobIDs)

        if not params["OK"] or not attrs["OK"]:
            return S_ERROR("Failed to load job data")

        jobParamsDict = params["Value"]
        jobAttrDict = attrs["Value"]

        records = []

        for jobID in jobParamsDict:
            rec = {}

            for k, v in jobParamsDict[jobID].items():
                if k in JOB_PARAMETER_KEYS:
                    rec[k] = v

            for k, v in jobAttrDict.get(jobID, {}).items():
                if k in JOB_ATTRIBUTE_KEYS:
                    if k in TIME_STAMPS and (v is None or str(v) == "None"):
                        rec.pop(k, None)
                    else:
                        rec[k] = str(v) if k in TIME_STAMPS else v

            rec["JobID"] = int(jobID)
            records.append(rec)

        successJobs = []

        # -------------------------------------------------------------
        # Process jobs
        # -------------------------------------------------------------

        startJobs = time.time()

        for rec in records:

            if (
                rec.get("Status") == JobStatus.FAILED
                and rec.get("EndExecTime") is None
            ):
                self.log.info(
                    f"Dropping failed JobID={rec['JobID']} without EndExecTime"
                )
                successJobs.append(rec["JobID"])
                continue

            startRecord = time.time()

            tdp, cores = self.__getProcessorParameters(
                rec.get("ModelName", "Unknown")
            )

            site = rec.get("Site", "Unknown")

            # ---- READ from CIM ----
            pue, ci, gocdb = self.cimClient.getSiteGreenMetrics(
                site,
                startExecTime=rec.get("StartExecTime"),
                endExecTime=rec.get("EndExecTime"),
            )

            self.log.debug(f"Time after getSiteGreenMetrics: {time.time()-startRecord}")

            rec["SiteDIRAC"] = site
            rec["SiteGOCDB"] = gocdb
            rec["Site"] = gocdb

            cpu_s = float(rec.get("TotalCPUTime(s)", 0))
            wallclock_s = float(rec.get("WallClockTime(s)", 0))

            # Default assumption: one core per process
            cores_used = 1

            energy_kwh = self.__compute_energy_kwh(
                cpu_seconds=cpu_s,
                wallclock_seconds=wallclock_s,
                tdp=tdp,
                total_cores=cores,
                cores_used=cores_used,
            )
            energy_wh = energy_kwh * 1000.0
            emissions = energy_kwh * pue * ci

            cpunorm = float(rec.get("CPUNormalizationFactor", 0))
            cee = (cpunorm * cores) / float(tdp) if tdp else 0.0

            # -------------------------------------------------
            # HTC metrics (added – minimal checks only)
            # -------------------------------------------------
            wallclock_s = float(rec.get("WallClockTime(s)", 0))
            norm_cpu_s = float(rec.get("NormCPUTime(s)", 0))

            if wallclock_s > 0:
                rec["Efficiency"] = norm_cpu_s / wallclock_s
            else:
                rec["Efficiency"] = 0.0

            if energy_wh > 0:
                rec["Work"] = norm_cpu_s / energy_wh
            else:
                rec["Work"] = 0.0

            rec.update({
                "ExecUnitID": rec["JobID"],
                "PUE": pue,
                "CI_g": ci,
                "Energy_wh": energy_wh,
                "CFP_g": emissions,
                "Owner": Registry.getVOForGroup(rec.get("OwnerGroup")),
                "ExecUnitFinished": 1,
                "NCores": cores,
                "TDP_w": tdp,
                "CEE": cee,
            })

            # -------------------------------------------------
            # SUBMIT to CIM
            # -------------------------------------------------

            startCIM = time.time()
            cimStored = False

            try:
                self.log.info(f"Submitting full record to CIM: {rec}")
                ok = self.cimClient.submitRecord(rec)
                if ok:
                    self.log.info(
                        f"CIM submission OK for JobID={rec['ExecUnitID']} "
                        f"Site={gocdb}; time spent {time.time() - startCIM}"
                    )
                    cimStored = True
                else:
                    self.log.error(
                        f"CIM submission FAILED for JobID={rec['ExecUnitID']}"
                    )
            except Exception as e:
                self.log.exception(
                    f"CIM submission EXCEPTION for JobID={rec['ExecUnitID']}: {e}"
                )

            # -------------------------------------------------
            # STORE in ElasticSearch
            # -------------------------------------------------
            greenMetricsStored = self.__storeJobGreenMetrics(rec)
            if greenMetricsStored:
                self.log.info(
                    f"ElasticSearch storage OK for JobID={rec['ExecUnitID']}"
                )

            # Mark a job processed only after both required destinations
            # accepted the record. Otherwise the next cycle retries it.
            if cimStored and greenMetricsStored:
                successJobs.append(rec["JobID"])

        # Mark processed
        self.log.info(f"Sending ApplicationNumStatus updates for {len(successJobs)} jobs")
        if not successJobs:
            return S_OK()

        result = self.jobDB.setJobAttributes(
            successJobs, ["ApplicationNumStatus"], [9999]
        )
        if not result["OK"]:
             self.log.error("Failed to update ApplicationNumStatus attributes")

        self.log.debug(f"Processing {len(records)} jobs in {time.time()-startJobs}")

        return result

    # =====================================================================
    # HELPERS
    # =====================================================================
    def __getProcessorParameters(self, model):
        if model in self.cpuDict:
            cpu = self.cpuDict[model]
            return cpu["TDP"], cpu["Cores"]
        self.log.warn(f"Unknown CPU model: {model}")
        return DEFAULT_TDP, 12

    def __compute_energy_kwh(self, cpu_seconds, wallclock_seconds, tdp, total_cores, cores_used=1):
        """
        Energy model (professor's formula):

        E = ((1-f)*CPUtime + f*WallClockTime)
            * (CoresUsed / TotalCores)
            * TDP

        Returned value is in kWh.
        """
        try:
            if wallclock_seconds <= 0 or total_cores <= 0:
                return 0.0

            f = IDLE_CONSUMPTION_FACTOR
            f = max(0.0, min(1.0, f))

            effective_time_s = (1.0 - f) * float(cpu_seconds) + f * float(wallclock_seconds)
            core_fraction = float(cores_used) / float(total_cores)

            energy_joule = effective_time_s * core_fraction * float(tdp)
            energy_kwh = energy_joule / 3_600_000.0

            return energy_kwh

        except Exception:
            return 0.0

    def __greenMetricsIndexName(self, sequence):
        # Keep the same million-index suffix style used by DIRAC's
        # ElasticJobParametersDB, for example "_233.0m".
        return f"{self.greenMetricsIndexBase}_{float(sequence):.1f}m"

    def __greenMetricsIndexSequence(self, indexName):
        """
        Extract the numeric rolling sequence from an index name.

        Accepted examples:
          dirac-in2p3-_greenmetrics_index_233m
          dirac-in2p3-_greenmetrics_index_233.0m
        """
        match = re.match(
            rf"^{re.escape(self.greenMetricsIndexBase)}_(\d+(?:\.\d+)?)m$",
            indexName,
        )
        if not match:
            return None
        return int(float(match.group(1)))

    def __getIndexDocumentCount(self, indexName):
        query = {
            "size": 0,
            "track_total_hits": True,
            "query": {"match_all": {}},
        }
        result = self.elasticJobParametersDB.query(index=indexName, query=query)
        if not result["OK"]:
            return result

        total = result["Value"].get("hits", {}).get("total", 0)
        if isinstance(total, dict):
            total = total.get("value", 0)
        return S_OK(int(total))

    def __initializeActiveGreenMetricsIndex(self):
        """
        Single-agent startup logic: select and count only the highest-numbered
        existing green-metrics index.
        """
        try:
            indexNames = self.elasticJobParametersDB.getIndexes(
                self.greenMetricsIndexBase
            )
        except Exception as exc:
            return S_ERROR(f"Cannot list green metrics indexes: {exc}")

        self.log.info(f"Discovered green metrics indexes: {indexNames}")
        if not indexNames:
            self._activeGreenMetricsIndex = self.__greenMetricsIndexName(0)
            self._activeGreenMetricsCount = 0
            self.log.info(
                "No green metrics indexes exist; first record will create "
                f"{self._activeGreenMetricsIndex}"
            )
            return S_OK(self._activeGreenMetricsIndex)

        indexedSequences = []
        for indexName in indexNames:
            sequence = self.__greenMetricsIndexSequence(indexName)
            if sequence is not None:
                indexedSequences.append((sequence, indexName))

        if not indexedSequences:
            return S_ERROR(
                f"No valid rolling indexes found for {self.greenMetricsIndexBase}; "
                f"discovered={indexNames}"
            )

        sequence, indexName = max(indexedSequences)
        self._ensuredGreenMetricsIndexes.add(indexName)

        count = self.__getIndexDocumentCount(indexName)
        if not count["OK"]:
            return count

        if count["Value"] >= self.greenMetricsMaxDocuments:
            self._activeGreenMetricsIndex = self.__greenMetricsIndexName(
                sequence + 1
            )
            self._activeGreenMetricsCount = 0
        else:
            self._activeGreenMetricsIndex = indexName
            self._activeGreenMetricsCount = count["Value"]

        self.log.info(
            "Selected active green metrics index "
            f"{self._activeGreenMetricsIndex} "
            f"(documents={self._activeGreenMetricsCount})"
        )
        return S_OK(self._activeGreenMetricsIndex)

    def __getWritableGreenMetricsIndex(self):
        if not self._activeGreenMetricsIndex:
            result = self.__initializeActiveGreenMetricsIndex()
            if not result["OK"]:
                return result

        if self._activeGreenMetricsCount >= self.greenMetricsMaxDocuments:
            sequence = self.__greenMetricsIndexSequence(
                self._activeGreenMetricsIndex
            )
            if sequence is None:
                return S_ERROR(
                    f"Invalid green metrics index name: "
                    f"{self._activeGreenMetricsIndex}"
                )
            self._activeGreenMetricsIndex = self.__greenMetricsIndexName(
                sequence + 1
            )
            self._activeGreenMetricsCount = 0

        return S_OK(self._activeGreenMetricsIndex)

    def __createGreenMetricsIndex(self, indexName):
        if indexName in self._ensuredGreenMetricsIndexes:
            return S_OK(indexName)

        result = self.elasticJobParametersDB.createIndex(
            indexName,
            GREEN_METRICS_MAPPING,
            period=None,
        )
        if result["OK"]:
            self._ensuredGreenMetricsIndexes.add(indexName)
            self.log.info(f"Using green metrics index {indexName}")
        return result

    def __storeJobGreenMetrics(self, record):
        jobID = record.get("ExecUnitID")
        if not jobID or not self.elasticJobParametersDB:
            return False

        indexResult = self.__getWritableGreenMetricsIndex()
        if not indexResult["OK"]:
            self.log.error(
                "Cannot select green metrics index: "
                f"{indexResult.get('Message')}"
            )
            return False
        indexName = indexResult["Value"]

        createResult = self.__createGreenMetricsIndex(indexName)
        if not createResult["OK"]:
            self.log.error(
                f"Cannot create green metrics index {indexName}: "
                f"{createResult.get('Message')}"
            )
            return False

        document = {
            key: (str(value) if key in TIME_STAMPS else value)
            for key, value in record.items()
            if value is not None
            and not (key in TIME_STAMPS and str(value) == "None")
        }
        document["timestamp"] = int(TimeUtilities.toEpochMilliSeconds())

        documentExists = self.elasticJobParametersDB.existsDoc(
            indexName,
            docID=str(jobID),
        )
        result = self.elasticJobParametersDB.index(
            indexName=indexName,
            body=document,
            docID=str(jobID),
        )
        if not result["OK"]:
            self.log.error(
                f"Green metrics write failed for JobID={jobID}, "
                f"index={indexName}: {result.get('Message')}"
            )
            return False

        if not documentExists:
            self._activeGreenMetricsCount += 1

        return True

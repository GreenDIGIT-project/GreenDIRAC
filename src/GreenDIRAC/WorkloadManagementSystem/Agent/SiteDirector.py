#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GreenSiteDirector — READ-ONLY green-aware SiteDirector

- Does NOT override beginExecution()
- Overrides _getSortedQueues()
- Adds explicit logs showing queue ordering
"""

from __future__ import annotations

from DIRAC import S_OK
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import (
    SiteDirector as BaseSiteDirector,
)

from GreenDIRAC.WorkloadManagementSystem.Client.CIMClient import CIMClient

import time

CEE_CACHE_TTL = 180  # seconds (10 minutes)


# =====================================================================
# CONSTANTS
# =====================================================================
DEFAULT_CEE = 1.0

ES_TIME_WINDOW = "now-30d"


# =====================================================================
# GREEN SITE DIRECTOR
# =====================================================================
class SiteDirector(BaseSiteDirector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.always("GreenSiteDirector loaded (GreenScore-based queue ordering)")
        self.cimClient = CIMClient(logger=self.log)

        # ---- CEE CACHE ----
        self._ceeCache = {}
        self._ceeCacheTimestamp = 0
        self.elasticJobParametersDB = None

    def _getElasticJobParametersDB(self):
        if self.elasticJobParametersDB:
            return self.elasticJobParametersDB

        res = ObjectLoader().loadObject(
            "WorkloadManagementSystem.DB.ElasticJobParametersDB",
            "ElasticJobParametersDB",
        )
        if not res["OK"]:
            self.log.error(f"Cannot load ElasticJobParametersDB: {res['Message']}")
            return None

        try:
            self.elasticJobParametersDB = res["Value"](parentLogger=self.log)
        except Exception as exc:
            self.log.error(f"Cannot initialize ElasticJobParametersDB: {exc}")
            return None

        return self.elasticJobParametersDB

    # =================================================================
    # QUEUE SORTING OVERRIDE (WITH LOGS)
    # =================================================================
    def _getSortedQueues(self):
        """
        Sort queues by GreenScore (descending) and log top 5.
        """
        # queueDict layout (input from BaseSiteDirector), example with 2 queues:
        # {
        #   "PARIS_CE01/queueA": {
        #     "Site": "EGI.IN2P3.fr",
        #     "CEName": "ce01.in2p3.fr",
        #     "ParametersDict": {
        #       "MaxCPUTime": 86400,
        #       "QueueStatus": "Production"
        #     }
        #   },
        #   "SARA_CE02/queueB": {
        #     "Site": "EGI.SARA.nl",
        #     "CEName": "ce02.surfsara.nl",
        #     "ParametersDict": {
        #       "MaxCPUTime": 43200,
        #       "QueueStatus": "Production"
        #     }
        #   }
        # }

        self.log.always(
            f"GreenSiteDirector: sorting {len(self.queueDict)} queues by GreenScore"
        )

        try:
            greenMetrics = self._computeGreenMetrics()
        except Exception as e:
            self.log.error(
                f"GreenSiteDirector: failed to compute green metrics, "
                f"falling back to random order: {e}"
            )
            return super()._getSortedQueues()

        # sortedQueues = sorted(
        #     self.queueDict.items(),
        #     key=lambda item: item[1]
        #     .get("ParametersDict", {})
        #     .get("GreenScore", 0.0),
        #     reverse=True,
        # )

        sortedQueues = sorted(
            self.queueDict.items(),
            key=lambda item: greenMetrics
            .get(item[0], {})
            .get("GreenScore", 0.0),
            reverse=True,
        )

        # ---- LOG TOP 5 SORTED QUEUES ----
        self.log.always("GreenSiteDirector: TOP 5 QUEUES AFTER GREEN SORT")
        for i, (qName, qDict) in enumerate(sortedQueues[:5], 1):
            gm = greenMetrics.get(qName, {})
            self.log.always(
                f"  {i}. {qName:<45} "
                f"GREEN={gm.get('GreenScore', 0.0):.4f} "
                f"PUE={gm.get('PUE')} "
                f"CI={gm.get('CI')} "
                f"CEE={gm.get('CEE')}"
            )
        return sortedQueues

    # =================================================================
    # GREEN METRICS COMPUTATION
    # =================================================================
    def _computeGreenMetrics(self):
        if not self.queueDict:
            return {}

        # avgCEE layout (from _getAverageCEEFromES cache):
        # {
        #   "<GridCE>": <float_avg_cee>,
        #   "...": <float_avg_cee>
        # }
        avgCEE = self._getAverageCEEFromES()
        greenMetrics = {}

        # Resolve PUE/CI once per unique site to avoid repeated calls for queues sharing a site.
        siteMetrics = {}
        uniqueSites = {qDict.get("Site", "") for qDict in self.queueDict.values()}
        for site in uniqueSites:
            try:
                pue, ci, _gocdb = self.cimClient.getSiteGreenMetrics(site)
            except Exception:
                pue, ci = 1.7, 500.0
            siteMetrics[site] = (pue, ci)

        for qName, qDict in self.queueDict.items():
            site = qDict.get("Site", "")
            ce = qDict.get("CEName", "")
            pue, ci = siteMetrics.get(site, (1.7, 500.0))

            # CEE
            cee = avgCEE.get(ce, DEFAULT_CEE)

            # GreenScore
            try:
                greenScore = float(cee) / (float(ci) * float(pue))
            except Exception:
                greenScore = 0.0

            greenMetrics[qName] = {
                "PUE": float(pue),
                "CI": float(ci),
                "CEE": float(cee),
                "GreenScore": greenScore,
            }

        # greenMetrics layout (output):
        # {
        #   "<QueueName>": {
        #     "PUE": <float>,
        #     "CI": <float>,
        #     "CEE": <float>,
        #     "GreenScore": <float>
        #   },
        #   "...": { ... }
        # }
        return greenMetrics

    # =================================================================
    # ELASTICSEARCH: AVERAGE CEE
    # =================================================================
    def _getAverageCEEFromES(self):
        now = time.time()

        # ---- CACHE HIT ----
        if (
                self._ceeCache
                and (now - self._ceeCacheTimestamp) < CEE_CACHE_TTL
        ):
            self.log.debug(
                "GreenSiteDirector: using cached CEE values "
                f"(age={int(now - self._ceeCacheTimestamp)}s)"
            )
            return self._ceeCache

        self.log.info("GreenSiteDirector: refreshing CEE cache from Elasticsearch")

        db = self._getElasticJobParametersDB()
        if not db:
            return self._ceeCache

        indexPattern = f"{db.indexName_base}_*"
        self.log.info(
            "GreenSiteDirector: querying ES via ElasticJobParametersDB "
            f"(host={getattr(db, '_dbHost', 'n/a')}, port={getattr(db, '_dbPort', 'n/a')}, "
            f"indexPattern={indexPattern}, window={ES_TIME_WINDOW})"
        )

        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": ES_TIME_WINDOW}}},
                        {"exists": {"field": "CEE"}},
                        {"exists": {"field": "GridCE"}},
                    ]
                }
            },
            "aggs": {
                "by_gridce": {
                    "terms": {"field": "GridCE", "size": 10000},
                    "aggs": {"avg_cee": {"avg": {"field": "CEE"}}},
                }
            },
        }
        # Elasticsearch aggregation query layout:
        # {
        #   "size": 0,
        #   "query": {
        #     "bool": {
        #       "filter": [
        #         {"range": {"timestamp": {"gte": "now-30d"}}},
        #         {"exists": {"field": "CEE"}},
        #         {"exists": {"field": "GridCE"}}
        #       ]
        #     }
        #   },
        #   "aggs": {
        #     "by_gridce": {
        #       "terms": {"field": "GridCE", "size": 10000},
        #       "aggs": {"avg_cee": {"avg": {"field": "CEE"}}}
        #     }
        #   }
        # }

        res = db.query(index=indexPattern, query=query)
        if not res["OK"]:
            self.log.error(f"GreenSiteDirector: ES aggregation failed: {res['Message']}")
            return self._ceeCache

        # Expected Elasticsearch response layout (subset used by SiteDirector):
        # {
        #   "took": <int_ms>,
        #   "timed_out": <bool>,
        #   "_shards": {
        #     "total": <int>,
        #     "successful": <int>,
        #     "skipped": <int>,
        #     "failed": <int>
        #   },
        #   "hits": {
        #     "total": {
        #       "value": <int_total_docs_matching_query>,
        #       "relation": "eq" | "gte"
        #     },
        #     "max_score": null,
        #     "hits": []   # empty because query uses "size": 0
        #   },
        #   "aggregations": {
        #     "by_gridce": {
        #       "doc_count_error_upper_bound": <int>,
        #       "sum_other_doc_count": <int>,
        #       "buckets": [
        #         {
        #           "key": "<GridCE>",
        #           "doc_count": <int_docs_for_this_gridce>,
        #           "avg_cee": {
        #             "value": <float_or_null>
        #           }
        #         }
        #       ]
        #     }
        #   }
        # }
        totalHits = (
            res["Value"]
            .get("hits", {})
            .get("total", {})
            .get("value")
        )
        buckets = (
            res["Value"].get("aggregations", {})
            .get("by_gridce", {})
            .get("buckets", [])
        )
        self.log.info(
            "GreenSiteDirector: ES aggregation response "
            f"(hits={totalHits}, buckets={len(buckets)})"
        )
        # ES response subset layout used here:
        # {
        #   "aggregations": {
        #     "by_gridce": {
        #       "buckets": [
        #         {
        #           "key": "<GridCE>",
        #           "avg_cee": {"value": <float_or_null>}
        #         }
        #       ]
        #     }
        #   }
        # }

        avgCEE = {}
        for b in buckets:
            if b.get("key") and b.get("avg_cee", {}).get("value") is not None:
                try:
                    avgCEE[b["key"]] = float(b["avg_cee"]["value"])
                except Exception:
                    pass

        # ---- UPDATE CACHE ----
        self._ceeCache = avgCEE
        self._ceeCacheTimestamp = now
        # _ceeCache layout:
        # {
        #   "<GridCE>": <float_avg_cee>,
        #   "...": <float_avg_cee>
        # }

        self.log.info(
            f"GreenSiteDirector: cached CEE for {len(avgCEE)} GridCEs "
            f"(indexPattern={indexPattern})"
        )

        return avgCEE

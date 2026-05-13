"""
Minimal CIM/KPI client for GreenDIRAC.
"""

import configparser
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from DIRAC.ConfigurationSystem.Client.Utilities import getDIRACGOCDictionary


DEFAULT_PUE = 1.7
DEFAULT_CI = 500
DEFAULT_ENERGY_WH = 8500

DEFAULT_TOKEN_MAX_AGE_H = 24
DEFAULT_CACHE_TTL = 300
DEFAULT_TOKEN_TIMEOUT_S = 20
DEFAULT_PUE_TIMEOUT_S = 20
DEFAULT_CI_TIMEOUT_S = 30
DEFAULT_SUBMIT_TIMEOUT_S = 30
DEFAULT_CACHE_MAX_ENTRIES = 5000
DEFAULT_STALE_MAX_AGE_S = 86400
DEFAULT_HTTP_RETRIES = 2
DEFAULT_HTTP_BACKOFF_S = 0.5


class CIMClient:
    def __init__(self, confFile=None, logger=None):
        self.log = logger
        self._token = None
        self._token_ts = None
        self._site_cache = {}  # (site, hour_bucket) -> (ts, pue, ci, gocdb)
        self._loadConfig(confFile)

    def _loadConfig(self, confFile):
        if not confFile:
            confFile = os.path.join(os.path.dirname(__file__), "cim.conf")
        if not os.path.exists(confFile):
            raise RuntimeError(f"CIMClient config file not found: {confFile}")

        cfg = configparser.ConfigParser()
        cfg.read(confFile)

        self.cim_email = cfg.get("CIM", "EMAIL")
        self.cim_password = cfg.get("CIM", "PASSWORD")
        self.cim_api_base = cfg.get("CIM", "API_BASE").rstrip("/")
        self.metrics_url = cfg.get("CIM", "METRICS_URL").rstrip("/")
        self.kpi_api_base = cfg.get("KPI", "API_BASE").rstrip("/")

        self.default_pue = cfg.getfloat("Defaults", "PUE", fallback=DEFAULT_PUE)
        self.default_ci = cfg.getfloat("Defaults", "CI", fallback=DEFAULT_CI)
        self.default_energy_wh = cfg.getint(
            "Defaults", "ENERGY_WH", fallback=DEFAULT_ENERGY_WH
        )

        self.token_max_age_h = cfg.getfloat(
            "Runtime", "TOKEN_MAX_AGE_H", fallback=DEFAULT_TOKEN_MAX_AGE_H
        )
        self.cache_ttl = cfg.getint("Runtime", "CACHE_TTL", fallback=DEFAULT_CACHE_TTL)
        self.token_timeout_s = cfg.getfloat(
            "Runtime", "TOKEN_TIMEOUT_S", fallback=DEFAULT_TOKEN_TIMEOUT_S
        )
        self.pue_timeout_s = cfg.getfloat(
            "Runtime", "PUE_TIMEOUT_S", fallback=DEFAULT_PUE_TIMEOUT_S
        )
        self.ci_timeout_s = cfg.getfloat(
            "Runtime", "CI_TIMEOUT_S", fallback=DEFAULT_CI_TIMEOUT_S
        )
        self.submit_timeout_s = cfg.getfloat(
            "Runtime", "SUBMIT_TIMEOUT_S", fallback=DEFAULT_SUBMIT_TIMEOUT_S
        )
        self.cache_max_entries = cfg.getint(
            "Runtime", "CACHE_MAX_ENTRIES", fallback=DEFAULT_CACHE_MAX_ENTRIES
        )
        self.stale_max_age_s = cfg.getint(
            "Runtime", "STALE_MAX_AGE_S", fallback=DEFAULT_STALE_MAX_AGE_S
        )
        self.http_retries = cfg.getint(
            "Runtime", "HTTP_RETRIES", fallback=DEFAULT_HTTP_RETRIES
        )
        self.http_backoff_s = cfg.getfloat(
            "Runtime", "HTTP_BACKOFF_S", fallback=DEFAULT_HTTP_BACKOFF_S
        )

    def _log(self, level, message):
        if self.log:
            getattr(self.log, level)(message)

    def _request(self, method, url, timeout, **kwargs):
        retries = max(0, int(self.http_retries))
        backoff = max(0.0, float(self.http_backoff_s))
        last_exc = None

        for attempt in range(retries + 1):
            try:
                resp = requests.request(method, url, timeout=timeout, **kwargs)
                if resp.status_code not in (429,) and resp.status_code < 500:
                    return resp
                if attempt == retries:
                    return resp
            except requests.RequestException as exc:
                last_exc = exc
                if attempt == retries:
                    raise

            if backoff > 0:
                time.sleep(backoff * (2 ** attempt))

        if last_exc:
            raise last_exc
        raise RuntimeError(f"HTTP request failed after retries: {method} {url}")

    @staticmethod
    def _to_float(value, default):
        try:
            return float(value), True
        except (TypeError, ValueError):
            return float(default), False

    def _as_iso8601_utc(self, value):
        if not value:
            return None

        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value).strip()
            if not text:
                return None
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                dt = None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue
                if dt is None:
                    return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

    def _hour_bucket(self, execTime):
        exec_iso = self._as_iso8601_utc(execTime)
        if not exec_iso:
            return "__default__"
        dt = datetime.fromisoformat(exec_iso.replace("Z", "+00:00"))
        dt = dt.replace(minute=0, second=0, microsecond=0)
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

    def _resolveGOCDB(self, site):
        try:
            res = getDIRACGOCDictionary()
            if res["OK"]:
                return res["Value"].get(site, site)
        except Exception:
            pass
        return site

    def _prune_cache(self, now):
        if not self._site_cache:
            return

        if self.stale_max_age_s > 0:
            old_keys = [
                key
                for key, (ts, _pue, _ci, _gocdb) in self._site_cache.items()
                if now - ts > self.stale_max_age_s
            ]
            for key in old_keys:
                del self._site_cache[key]

        if self.cache_max_entries > 0 and len(self._site_cache) > self.cache_max_entries:
            overflow = len(self._site_cache) - self.cache_max_entries
            oldest = sorted(self._site_cache.items(), key=lambda item: item[1][0])[
                :overflow
            ]
            for key, _value in oldest:
                del self._site_cache[key]

    # def _prune_cache(self, now):
    #     """
    #     Site-aware prune policy:
    #     - Keep at least ONE cached entry per site (fresh or stale).
    #     - Remove stale/overflow entries only when a site has >1 entries.
    #     - If every remaining entry is the last one for its site, allow temporary overflow
    #       instead of evicting the final fallback.
    #     """
    #     if not self._site_cache:
    #         return
    #
    #     def _site_counts():
    #         counts = {}
    #         for (site, _bucket) in self._site_cache:
    #             counts[site] = counts.get(site, 0) + 1
    #         return counts
    #
    #     # -----------------------------
    #     # 1) Age-based prune (safe)
    #     # -----------------------------
    #     if self.stale_max_age_s > 0:
    #         site_counts = _site_counts()
    #         old_entries = sorted(
    #             self._site_cache.items(), key=lambda item: item[1][0]
    #         )  # oldest first
    #
    #         for key, (ts, _pue, _ci, _gocdb) in old_entries:
    #             age = now - ts
    #             if age <= self.stale_max_age_s:
    #                 continue
    #
    #             site = key[0]
    #             # delete only if site keeps at least one entry after deletion
    #             if site_counts.get(site, 0) > 1:
    #                 del self._site_cache[key]
    #                 site_counts[site] -= 1
    #
    #     # -----------------------------
    #     # 2) Size-based prune (safe)
    #     # -----------------------------
    #     if self.cache_max_entries > 0 and len(self._site_cache) > self.cache_max_entries:
    #         site_counts = _site_counts()
    #         candidates = sorted(
    #             self._site_cache.items(), key=lambda item: item[1][0]
    #         )  # oldest first
    #
    #         # Evict oldest entries, but never the last one for a site
    #         for key, (_ts, _pue, _ci, _gocdb) in candidates:
    #             if len(self._site_cache) <= self.cache_max_entries:
    #                 break
    #
    #             site = key[0]
    #             if site_counts.get(site, 0) > 1:
    #                 del self._site_cache[key]
    #                 site_counts[site] -= 1
    #
    #         # Optional: if still over limit, keep overflow rather than dropping
    #         # the last fallback for any site.
    #         if len(self._site_cache) > self.cache_max_entries:
    #             self._log(
    #                 "warn",
    #                 f"Cache above max_entries={self.cache_max_entries}, "
    #                 "kept overflow to preserve one fallback per site",
    #             )

    def _get_stale_fallback(self, site, cache_key, now):
        exact = self._site_cache.get(cache_key)
        if exact:
            ts, pue, ci, gocdb = exact
            age = now - ts
            if age < self.cache_ttl:
                return "fresh", (pue, ci, gocdb)
            return "stale", (pue, ci, gocdb, age, cache_key)

        newest = None
        for (cached_site, cached_bucket), (ts, pue, ci, gocdb) in self._site_cache.items():
            if cached_site != site:
                continue
            if newest is None or ts > newest[0]:
                newest = (ts, pue, ci, gocdb, cached_bucket)

        if not newest:
            return None, None

        ts, pue, ci, gocdb, cached_bucket = newest
        return "stale", (pue, ci, gocdb, now - ts, (site, cached_bucket))

    def _getToken(self):
        if self._token and self._token_ts:
            age_h = (time.time() - self._token_ts) / 3600.0
            if age_h < self.token_max_age_h:
                return self._token

        url = f"{self.cim_api_base}/token"
        resp = self._request(
            "GET",
            url,
            self.token_timeout_s,
            params={"email": self.cim_email, "password": self.cim_password},
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"CIM authentication failed: status={resp.status_code}")

        token = None
        try:
            data = resp.json()
            if isinstance(data, dict):
                token = data.get("access_token")
            elif data is not None:
                token = str(data).strip().strip('"')
        except ValueError:
            token = resp.text.strip().strip('"')

        if not token:
            raise RuntimeError("CIM authentication failed: empty token response")

        self._token = token
        self._token_ts = time.time()
        return token

    def _queryPUEandCI(self, gocdb, startExecTime=None, endExecTime=None):
        token = self._getToken()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        pue_resp = self._request(
            "POST",
            f"{self.kpi_api_base}/pue",
            self.pue_timeout_s,
            json={"site_name": gocdb},
            headers=headers,
        )
        pue_resp.raise_for_status()
        pue_data = pue_resp.json()

        pue, pue_ok = self._to_float(pue_data.get("pue", self.default_pue), self.default_pue)
        cacheable = pue_ok

        location = pue_data.get("location") or {}
        lat = location.get("latitude")
        lon = location.get("longitude")
        if lat is None or lon is None:
            return pue, float(self.default_ci), False

        now = datetime.now(timezone.utc)
        end_iso = self._as_iso8601_utc(endExecTime) or now.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )
        start_iso = self._as_iso8601_utc(startExecTime) or (
            now - timedelta(hours=2)
        ).isoformat(timespec="seconds").replace("+00:00", "Z")

        ci_resp = self._request(
            "POST",
            f"{self.kpi_api_base}/ci",
            self.ci_timeout_s,
            json={
                "lat": lat,
                "lon": lon,
                "pue": pue,
                "energy_wh": self.default_energy_wh,
                "start": start_iso,
                "end": end_iso,
                "metric_id": gocdb,
            },
            headers=headers,
        )

        if ci_resp.status_code != 200:
            return pue, float(self.default_ci), False

        try:
            ci_data = ci_resp.json()
        except Exception:
            return pue, float(self.default_ci), False

        ci, ci_ok = self._to_float(ci_data.get("ci_gco2_per_kwh"), self.default_ci)
        return pue, ci, cacheable and ci_ok

    def getSiteGreenMetrics(self, site, startExecTime=None, endExecTime=None):
        now = time.time()
        self._prune_cache(now)
        cache_key = (site, self._hour_bucket(endExecTime))

        state, cached = self._get_stale_fallback(site, cache_key, now)
        if state == "fresh":
            return cached
        stale_fallback = cached if state == "stale" else None

        gocdb = self._resolveGOCDB(site)
        try:
            pue, ci, cacheable = self._queryPUEandCI(
                gocdb, startExecTime=startExecTime, endExecTime=endExecTime
            )
        except Exception as exc:
            self._log("error", f"CIMClient failure for site={site} gocdb={gocdb}: {exc}")
            pue, ci, cacheable = self.default_pue, self.default_ci, False

        if cacheable:
            self._site_cache[cache_key] = (now, pue, ci, gocdb)
            return pue, ci, gocdb

        if stale_fallback is not None:
            stale_pue, stale_ci, stale_gocdb, _stale_age_s, _stale_key = stale_fallback
            return stale_pue, stale_ci, stale_gocdb

        return pue, ci, gocdb

    def submitRecord(self, record):
        resp = self._request(
            "POST",
            self.metrics_url,
            self.submit_timeout_s,
            json=record,
            headers={
                "Authorization": f"Bearer {self._getToken()}",
                "Content-Type": "application/json",
            },
        )
        if resp.status_code not in (200, 201):
            self._log("error", f"CIM submission failed [{resp.status_code}]: {resp.text}")
            return False
        return True

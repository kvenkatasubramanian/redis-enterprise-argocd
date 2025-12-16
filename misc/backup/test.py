#!/usr/bin/env python3
import argparse
import datetime as dt
import time
import json
from typing import Dict, List, Optional, Tuple

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL_DEFAULT = "https://34.67.65.31:9443/v1/logs"

EXPECTED_TYPES = [
    "bdb_export_request",
    "bdb_export_started",
    "bdb_export_succeeded",
]

def parse_iso(ts: str) -> dt.datetime:
    # Handle Zulu times (e.g., 2025-10-24T16:41:14Z)
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return dt.datetime.fromisoformat(ts)

def fetch_logs(
    url: str,
    user: str,
    pwd: str,
    order: str = "desc",
    timeout_s: int = 10,
    verify_ssl: bool = False,
) -> List[Dict]:
    resp = requests.get(
        url,
        params={"order": order},
        auth=(user, pwd),
        timeout=timeout_s,
        verify=verify_ssl,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("API response is not a JSON list.")
    return data

def normalize_bdb_uid(val) -> str:
    # Some entries have "1" (string), others have 1 (int)
    return str(val) if val is not None else ""

def matches_destination(event: Dict, bucket_name: str, region_name: str) -> bool:
    dest = event.get("destination") or {}
    return (
        dest.get("bucket_name") == bucket_name
        and dest.get("region_name") == region_name
    )

def extract_first_sequence(
    logs: List[Dict],
    bdb_uid: str,
    bucket_name: str,
    region_name: str,
) -> Tuple[Optional[Dict], Optional[Dict], Optional[Dict]]:
    """
    Given logs in DESC time order, find the first COMPLETE sequence that matches:
      request (bdb_uid only) -> started (bdb_uid + destination) -> succeeded (same destination)
    All three must be in chronological order (request <= started <= succeeded).
    We return the dicts for (request, started, succeeded). Any missing remains None.
    """
    # Because logs are descending, we'll scan and try to assemble sequences keyed by the
    # destination (bucket, region) and bdb_uid. We only care about the FIRST (most recent)
    # completion that matches the user's bucket/region.
    request_evt = None
    started_evt = None
    succeeded_evt = None

    # Track by time to ensure order
    req_time = None
    start_time = None
    succ_time = None

    for ev in logs:
        ev_type = ev.get("type")
        ev_bdb = normalize_bdb_uid(ev.get("bdb_uid"))
        if ev_bdb != bdb_uid:
            continue

        # We only consider started/succeeded that match the destination filter
        if ev_type == "bdb_export_succeeded" and matches_destination(ev, bucket_name, region_name):
            # First matching succeeded we see (since descending) anchors the sequence
            if succeeded_evt is None:
                succeeded_evt = ev
                succ_time = parse_iso(ev["time"])
                # keep scanning for started and request older than/at succ_time
                continue

        if ev_type == "bdb_export_started" and matches_destination(ev, bucket_name, region_name):
            # Must be at or before succ_time (if succ found)
            t = parse_iso(ev["time"])
            if started_evt is None and (succ_time is None or t <= succ_time):
                started_evt = ev
                start_time = t
                continue

        if ev_type == "bdb_export_request":
            # Request has no destination, just bdb_uid. Must be at/before started if started present,
            # else at/before succeeded if only succeeded present (rare), else accept as most recent.
            t = parse_iso(ev["time"])
            # If we already have start_time, require t <= start_time.
            if request_evt is None:
                if start_time is not None:
                    if t <= start_time:
                        request_evt = ev
                        req_time = t
                elif succ_time is not None:
                    if t <= succ_time:
                        request_evt = ev
                        req_time = t
                else:
                    # no started/succeeded found yet; tentatively take it, but it might be superseded
                    # by a later (older in time) request once we find started/succeeded anchors.
                    request_evt = ev
                    req_time = t

        # Early exit if we have a full, correctly ordered trio
        if request_evt and started_evt and succeeded_evt:
            if req_time <= start_time <= succ_time:
                return request_evt, started_evt, succeeded_evt

    # If we exit the loop without a fully ordered trio, return partials (could be None)
    return request_evt, started_evt, succeeded_evt

def summarize_event(ev: Dict) -> Dict:
    return {
        "type": ev.get("type"),
        "time": ev.get("time"),
        "originator_email": ev.get("originator_email"),
        "originator_username": ev.get("originator_username"),
    }

def main():
    parser = argparse.ArgumentParser(description="Poll logs for an export sequence.")
    parser.add_argument("--url", default=API_URL_DEFAULT, help="API base URL (default: https://localhost:9443/v1/logs)")
    parser.add_argument("--user", required=True, help="Basic auth username")
    parser.add_argument("--pwd", required=True, help="Basic auth password")
    parser.add_argument("--bdb_uid", required=True, help='bdb_uid to filter (e.g., "1")')
    parser.add_argument("--bucket_name", required=True, help='Destination bucket_name (e.g., "kumar-demo")')
    parser.add_argument("--region_name", required=True, help='Destination region_name (e.g., "34.67.65.31/testdb/20251024114104")')
    parser.add_argument("--max_wait_minutes", type=int, default=15, help="Maximum wait time in minutes (default: 15)")
    parser.add_argument("--poll_interval_seconds", type=int, default=10, help="Polling interval in seconds (default: 10)")
    parser.add_argument("--insecure", action="store_true", help="Skip TLS verification (like curl -k). Default is secure.")
    args = parser.parse_args()

    deadline = time.time() + args.max_wait_minutes * 60
    verify_ssl = not args.insecure

    partial_log = {}  # to report what we’ve found so far

    while True:
        try:
            logs = fetch_logs(
                url=args.url,
                user=args.user,
                pwd=args.pwd,
                order="desc",
                timeout_s=15,
                verify_ssl=verify_ssl,
            )
            print(f"Fetched {len(logs)} log entries.")
        except Exception as e:
            print(f"ERROR: Failed to fetch logs: {e}")
            # If we can't fetch, don't spin too fast
            if time.time() >= deadline:
                print("Maximum wait time expired due to repeated fetch errors.")
                break
            time.sleep(args.poll_interval_seconds)
            continue

        req, started, succ = extract_first_sequence(
            logs,
            bdb_uid=str(args.bdb_uid),
            bucket_name=args.bucket_name,
            region_name=args.region_name,
        )

        # Keep a running note of what we have so far
        partial_log = {
            "bdb_uid": str(args.bdb_uid),
            "bucket_name": args.bucket_name,
            "region_name": args.region_name,
            "found": {
                "bdb_export_request": summarize_event(req) if req else None,
                "bdb_export_started": summarize_event(started) if started else None,
                "bdb_export_succeeded": summarize_event(succ) if succ else None,
            }
        }

        # If fully found and in the correct order, print and exit
        if req and started and succ:
            # Double-check order
            t_req = parse_iso(req["time"])
            t_started = parse_iso(started["time"])
            t_succ = parse_iso(succ["time"])
            if t_req <= t_started <= t_succ:
                print(json.dumps({
                    "status": "complete",
                    "sequence": [
                        summarize_event(req),
                        summarize_event(started),
                        summarize_event(succ),
                    ],
                    "details": {
                        "bdb_uid": str(args.bdb_uid),
                        "bucket_name": args.bucket_name,
                        "region_name": args.region_name,
                    }
                }, indent=2))
                return

        # Not complete yet: check deadline
        if time.time() >= deadline:
            print(json.dumps({
                "status": "timeout",
                "message": "Maximum wait time expired.",
                "progress": partial_log
            }, indent=2))
            return

        # Wait and try again
        time.sleep(args.poll_interval_seconds)

if __name__ == "__main__":
    main()

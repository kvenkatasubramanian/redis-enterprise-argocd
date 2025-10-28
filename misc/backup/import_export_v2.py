import requests
import json
import sys
import os
import boto3
import botocore
import csv
import logging
import argparse
import time
import datetime as dt
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
log_file = f"db_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    INFO = '\033[94m'
    ORANGE = '\033[93m'
    RESET = '\033[0m'

def now_folder() -> str:
    return f"{datetime.now().strftime('%Y%m%d%H%M%S')}"

def confirm(prompt):
    while True:
        user_input = input(prompt + " [y/n]: ").strip().lower()
        if user_input in {'y', 'yes'}:
            return True
        elif user_input in {'n', 'no'}:
            return False
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

def delete_objects(s3, bucketname, prefix):
    response = s3.list_objects_v2(Bucket=bucketname, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucketname, Key=obj['Key'])

def get_db_info(hostname, port, uid, auth):
    url = f"https://{hostname}:{port}/v1/bdbs/{uid}/command"
    headers = {"Content-Type": "application/json"}
    data = {"command": "INFO"}
    try:
        info_response = requests.post(url, auth=auth, headers=headers, json=data, verify=False)
        info_response.raise_for_status()
        info_data = info_response.json()
        db0_info = info_data['response'].get('db0')
        no_of_keys = int(db0_info.get('keys', 0)) if db0_info else 0
        return {'uid': uid, 'no_of_keys': no_of_keys}
    except Exception as e:
        logging.error(f"Error fetching info for database {uid}: {e}")
        print(Color.RED + f"Error fetching info for database {uid}: {e}" + Color.RESET)
        return {}

def read_csv_parameters(csv_file):
    params_list = []
    try:
        with open(csv_file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # convert port to int
                row['port'] = int(row['port'])
                params_list.append(row)
        logging.info(f"Loaded {len(params_list)} sets of parameters from CSV")
        print(Color.INFO + f"Loaded {len(params_list)} sets of parameters from CSV" + Color.RESET)
        return params_list
    except Exception as e:
        logging.error(f"Failed to read CSV {csv_file}: {e}")
        print(Color.RED + f"Failed to read CSV {csv_file}: {e}" + Color.RESET)
        sys.exit(1)

def parse_iso(ts: str) -> dt.datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return dt.datetime.fromisoformat(ts)

def fetch_logs(url, user, pwd, order="desc", timeout_s=10, verify_ssl=False):
    try:
        resp = requests.get(url, params={"order": order}, auth=(user, pwd), timeout=timeout_s, verify=verify_ssl)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError("API response is not a JSON list.")
        return data
    except Exception as e:
        logging.error(f"Failed to fetch logs: {e}")
        return []

def normalize_bdb_uid(val):
    return str(val) if val is not None else ""

def matches_destination(event, bucket_name, region_name):
    dest = event.get("destination") or {}
    return dest.get("bucket_name") == bucket_name and dest.get("region_name") == region_name

def extract_first_sequence(logs, bdb_uid, bucket_name, region_name):
    request_evt = started_evt = succeeded_evt = None
    req_time = start_time = succ_time = None

    for ev in logs:
        ev_type = ev.get("type")
        ev_bdb = normalize_bdb_uid(ev.get("bdb_uid"))
        if ev_bdb != str(bdb_uid):
            continue
                        
        if ev_type == "bdb_export_succeeded" and matches_destination(ev, bucket_name, region_name):
            if succeeded_evt is None:
                succeeded_evt = ev
                succ_time = parse_iso(ev["time"])
                continue

        if ev_type == "bdb_export_started" and matches_destination(ev, bucket_name, region_name):
            t = parse_iso(ev["time"])
            if started_evt is None and (succ_time is None or t <= succ_time):
                started_evt = ev
                start_time = t
                continue

        if ev_type == "bdb_export_request":
            t = parse_iso(ev["time"])
            if request_evt is None:
                if start_time is not None and t <= start_time:
                    request_evt = ev
                    req_time = t
                elif succ_time is not None and t <= succ_time:
                    request_evt = ev
                    req_time = t
                elif start_time is None and succ_time is None:
                    request_evt = ev
                    req_time = t

        if request_evt and started_evt and succeeded_evt:
            if req_time <= start_time <= succ_time:
                return request_evt, started_evt, succeeded_evt

    return request_evt, started_evt, succeeded_evt

def summarize_event(ev):
    if ev is None:
        return None
    return {
        "type": ev.get("type"),
        "time": ev.get("time"),
        "originator_email": ev.get("originator_email"),
        "originator_username": ev.get("originator_username"),
    }


# Check Redis logs for export sequence
def check_redis_log(user, pwd, url, bdb_uid, bucket_name, region_name, max_wait_minutes=15, poll_interval_seconds=10, verify_ssl=False):
    """
    Poll Redis Enterprise logs for the export sequence of a specific BDB UID and bucket/region.
    Logs INFO, WARNING, ERROR messages using logging module.
    """
    EXPECTED_TYPES = ["bdb_export_request", "bdb_export_started", "bdb_export_succeeded"]


    deadline = time.time() + max_wait_minutes * 60
    partial_log = {}

    logging.info(f"Checking Redis logs for bdb_uid={bdb_uid}, bucket={bucket_name}, region={region_name}")

    seen_request = False
    seen_started = False
    seen_succeeded = False

    while True:
        try:
            logs = fetch_logs(
                url=url,
                user=user,
                pwd=pwd,
                order="desc",
                timeout_s=15,
                verify_ssl=verify_ssl,
            )
        except Exception as e:
            logging.error(f"Failed to fetch logs: {e}")
            if time.time() >= deadline:
                logging.error("Maximum wait time expired due to repeated fetch errors.")
                return False
            time.sleep(poll_interval_seconds)
            continue

        req, started, succ = extract_first_sequence(
            logs,
            bdb_uid=str(bdb_uid),
            bucket_name=bucket_name,
            region_name=region_name,
        )

        # Log each event type when first detected
        if req and not seen_request:
            seen_request = True
            logging.info(f"Found {EXPECTED_TYPES[0]} event at {req.get('time')} for bdb_uid={bdb_uid}, by {req.get('originator_email')}, Role: {req.get('originator_username')}")

        if started and not seen_started:
            seen_started = True
            logging.info(f"Found {EXPECTED_TYPES[1]} event at {started.get('time')} for bdb_uid={bdb_uid}")

        if succ and not seen_succeeded:
            seen_succeeded = True
            logging.info(f"Found {EXPECTED_TYPES[2]} event at {succ.get('time')} for bdb_uid={bdb_uid}")

        # If all three events are found in order, mark success
        if req and started and succ:
            t_req = parse_iso(req["time"])
            t_started = parse_iso(started["time"])
            t_succ = parse_iso(succ["time"])
            if t_req <= t_started <= t_succ:
                logging.info("Export sequence completed successfully in Redis logs.")
                return True
            else:
                logging.warning("Event timestamps are out of order; continuing to poll...")

        # Timeout condition
        if time.time() >= deadline:
            missing = [t for t, s in zip(EXPECTED_TYPES, [seen_request, seen_started, seen_succeeded]) if not s]
            logging.warning(f"Timeout waiting for export completion. Missing events: {missing}")
            return False

        logging.info("Waiting for next log poll...")
        time.sleep(poll_interval_seconds)

def main():
    parser = argparse.ArgumentParser(description='Export or Import databases to/from AWS S3 using CSV input')
    parser.add_argument('-csv', required=True, help='Path to input CSV file with parameters')

    args = parser.parse_args()
    csv_file = args.csv

    # Check if the file exists
    if not os.path.isfile(csv_file):
        print(Color.RED + f"ERROR: CSV file '{csv_file}' does not exist." + Color.RESET)
        parser.print_help()
        sys.exit(1)

    params_list = read_csv_parameters(csv_file)    

    # Check AWS environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    if not aws_access_key_id or not aws_secret_access_key:
        logging.error("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
        print(Color.RED + "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set"+ Color.RESET)
        sys.exit(1)

    for params in params_list:
        hostname = params['hostname']
        userid = params['user']
        password = params['password']
        bucketname = params['bucket']
        port = params['port']
        operation = params['operation']
        dbname = params['dbname']

        auth = (userid, password)
        url = f"https://{hostname}:{port}/v1/bdbs"

        try:
            response = requests.get(url, auth=auth, verify=False)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logging.error(f"Failed to fetch DB list from {hostname}: {e}")
            print(Color.RED + f"Failed to fetch DB list from {hostname}: {e}" + Color.RESET)
            continue

        # Extract database info
        db_info = {}
        for item in data:
            if item['name'] == dbname:
                uid = item['uid']
                db_info[dbname] = get_db_info(hostname, port, uid, auth)

        if dbname not in db_info:
            logging.warning(f"Database '{dbname}' does not exist on {hostname}")
            print(Color.ORANGE + f"Database '{dbname}' does not exist on {hostname}" + Color.RESET)
            continue

        # Export operation
        if operation == 'export':
            for db_name, db_details in db_info.items():
                uid = db_details['uid']
                no_of_keys = db_details['no_of_keys']
                subdir = now_folder()               

                logging.info(f"Database: {db_name}, total_keys: {no_of_keys}")
                print(Color.INFO + f"Database: {db_name}, total_keys: {no_of_keys}" + Color.RESET)

                s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
                try:
                    s3.head_object(Bucket=bucketname, Key=f"{hostname}/{db_name}/{subdir}/")
                    logging.info(f"S3 bucket path exists: {bucketname}/{hostname}/{db_name}/{subdir}/")
                    print(Color.INFO + f"S3 bucket path exists: {bucketname}/{hostname}/{db_name}/{subdir}/" + Color.RESET)
                    if confirm(f"S3 Bucket '{bucketname}/{hostname}/{db_name}/{subdir}/' already contains data. Delete existing?"):
                        delete_objects(s3, bucketname, f"{hostname}/{db_name}/{subdir}/")
                        s3.put_object(Bucket=bucketname, Key=f"{hostname}/{db_name}/{subdir}/")
                    else:
                        logging.warning("Operation canceled by user")
                        print(Color.ORANGE + "Operation canceled by user" + Color.RESET)
                        continue
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        s3.put_object(Bucket=bucketname, Key=f"{hostname}/{db_name}/{subdir}/")
                    else:
                        logging.error(f"S3 error: {e}")
                        print(Color.RED + f"S3 error: {e}" + Color.RESET)
                        continue

                # Export DB
                export_data = {
                    "export_location": {
                        "type": "s3",
                        "bucket_name": bucketname,
                        "subdir": f"{hostname}/{db_name}/{subdir}",
                        "access_key_id": aws_access_key_id,
                        "secret_access_key": aws_secret_access_key
                    }
                }
                try:
                    response = requests.post(
                        f"https://{hostname}:{port}/v1/bdbs/{uid}/actions/export",
                        auth=auth,
                        headers={'Content-Type': 'application/json'},
                        json=export_data,
                        verify=False
                    )
                    response.raise_for_status()
                    # Check Redis logs for export sequence
                    if check_redis_log(
                            user=userid,
                            pwd=password,
                            url=f"https://{hostname}:{port}/v1/logs",
                            bdb_uid=uid,
                            bucket_name=bucketname,
                            region_name=f"{hostname}/{db_name}/{subdir}",
                            max_wait_minutes=15,
                            poll_interval_seconds=10,
                            verify_ssl=False
                        ):
                        logging.info(f"Verified Redis log: Export succeeded for database {db_name}")
                        print(Color.GREEN + f"Verified Redis log: Export succeeded for database {db_name}" + Color.RESET)
                        print(Color.GREEN + f"Redis export completed successfully. See log: {log_file}" + Color.RESET)
                    else:
                        logging.error(f"Redis log verification failed or timed out for database {db_name}")
                        print(Color.RED + f"Redis log verification failed or timed out for database {db_name}" + Color.RESET)
                        print(Color.RED + f"Export did not complete. Check log for details: {log_file}" + Color.RESET)
                        
                except Exception as e:
                    logging.error(f"Export failed for {db_name}: {e}")
                    print(Color.RED + f"Export failed for {db_name}: {e}" + Color.RESET)

if __name__ == "__main__":
    main()

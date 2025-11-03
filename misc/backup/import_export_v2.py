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

def aaextract_first_sequence(logs, bdb_uid, bucket_name, region_name):
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

def extract_first_sequence(logs, bdb_uid, bucket_name, region_name, operation):
    """
    Extracts the first valid (request, started, succeeded) event sequence for a given BDB UID.
    Works for both 'export' and 'import' operations even when logs are fetched in descending order.
    """

    request_evt = started_evt = succeeded_evt = None
    req_time = start_time = succ_time = None

    # Define event names based on operation
    if operation == "export":
        EVT_REQUEST = "bdb_export_request"
        EVT_STARTED = "bdb_export_started"
        EVT_SUCCEEDED = "bdb_export_succeeded"
        expected_order = ("request", "started", "succeeded")
    else:  # import
        EVT_REQUEST = "bdb_import_request"
        EVT_STARTED = "bdb_import_started"
        EVT_SUCCEEDED = "bdb_import_succeeded"
        expected_order = ("started", "request", "succeeded")

    # Iterate through logs
    for ev in logs:
        ev_type = ev.get("type")
        ev_bdb = normalize_bdb_uid(ev.get("bdb_uid"))
        if ev_bdb != str(bdb_uid):
            continue

        t = parse_iso(ev["time"])

        if ev_type == EVT_SUCCEEDED and matches_destination(ev, bucket_name, region_name):
            if not succeeded_evt:
                succeeded_evt = ev
                succ_time = t

        elif ev_type == EVT_STARTED and matches_destination(ev, bucket_name, region_name):
            if not started_evt:
                started_evt = ev
                start_time = t

        elif ev_type == EVT_REQUEST:
            if not request_evt:
                request_evt = ev
                req_time = t

        # --- Check sequence completion ---
        if request_evt and started_evt and succeeded_evt:
            times = {
                "request": req_time,
                "started": start_time,
                "succeeded": succ_time,
            }

            # Sort times chronologically to verify order
            ordered = sorted(times.items(), key=lambda x: x[1])
            order = [x[0] for x in ordered]

            # Check if matches expected event order for operation
            if order == list(expected_order):
                # Return always in (req, started, succ) order for consistency
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
def check_redis_log(user, pwd, url, bdb_uid, bucket_name, region_name,operation, max_wait_minutes=15, poll_interval_seconds=10, verify_ssl=False):
    """
    Poll Redis Enterprise logs for the import/export sequence of a specific BDB UID and bucket/region.
    Logs INFO, WARNING, ERROR messages using logging module.
    """
    EXPECTED_TYPES = ["bdb_export_request", "bdb_export_started", "bdb_export_succeeded"]
    if operation.lower() == "import":
        EXPECTED_TYPES = [ "bdb_import_started", "bdb_import_request", "bdb_import_succeeded"]
    else:
        EXPECTED_TYPES = ["bdb_export_request", "bdb_export_started", "bdb_export_succeeded"]


    deadline = time.time() + max_wait_minutes * 60
    partial_log = {}

    logging.info(
        f"Checking Redis logs for operation={operation}, bdb_uid={bdb_uid}, "
        f"bucket={bucket_name}, region={region_name}"
    )
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
            operation=operation
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

            print(f"t_started: {t_started}, t_req: {t_req}, t_succ: {t_succ}")
            if operation.lower() == "import":
                if t_started <= t_req <= t_succ:                
                    logging.info(f"{operation.capitalize()} sequence completed successfully in Redis logs.")
                    return True
                else:
                    logging.warning("Event timestamps are out of order; continuing to poll...")

            else:    
                if t_req <= t_started <= t_succ:                
                    logging.info(f"{operation.capitalize()} sequence completed successfully in Redis logs.")
                    return True
                else:
                    logging.warning("Event timestamps are out of order; continuing to poll...")

        # Timeout condition
        if time.time() >= deadline:
            missing = [t for t, s in zip(EXPECTED_TYPES, [seen_request, seen_started, seen_succeeded]) if not s]
            logging.warning(f"Timeout waiting for {operation} completion. Missing events: {missing}")
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
        timestamp = params.get('timestamp', '').strip()

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
                            operation=operation,
                            max_wait_minutes=15,
                            poll_interval_seconds=10,
                            verify_ssl=False
                        ):
                        logging.info(f"Verified Redis log: {operation.capitalize()} succeeded for database {db_name}")
                        print(Color.GREEN + f"Verified Redis log: {operation.capitalize()} succeeded for database {db_name}" + Color.RESET)
                        print(Color.GREEN + f"Redis {operation.capitalize()} completed successfully. See log: {log_file}" + Color.RESET)
                    else:
                        logging.error(f"Redis log verification failed or timed out for database {db_name}")
                        print(Color.RED + f"Redis log verification failed or timed out for database {db_name}" + Color.RESET)
                        print(Color.RED + f"{operation.capitalize()} did not complete. Check log for details: {log_file}" + Color.RESET)
                        
                except Exception as e:
                    logging.error(f"{operation.capitalize()} failed for {db_name}: {e}")
                    print(Color.RED + f"{operation.capitalize()} failed for {db_name}: {e}" + Color.RESET)
        elif operation == 'import':
            print(Color.INFO + f"Starting import operation for dbname:{dbname} with timestamp:{timestamp}..." + Color.RESET)
            logging.info(f"Starting import operation for dbname:{dbname} with timestamp:{timestamp}...")
            # Create subdirectories in S3
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            try:
                s3.head_bucket(Bucket=bucketname)            
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logging.error(f"Subdirectories {hostname} does not exist.")
                    print(Color.RED + f'Subdirectories {hostname} does not exist.' + Color.RESET)
                    sys.exit(1)
                else:
                    raise        
            
            # List directories under the bucket
            paginator = s3.get_paginator('list_objects_v2')
            result = paginator.paginate(Bucket=bucketname, Prefix=hostname)
            databases = {}

            # Add all databases
            for page in result:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        tmp_db_name = key.split('/')[1]    #db_name
                        tmp_timestamp = key.split('/')[2]  #timestamp
                        
                        if dbname == tmp_db_name and timestamp == tmp_timestamp:
                            if dbname not in databases:
                                databases[dbname] = set()
                            if obj['Key'].split('/')[3]:
                                databases[dbname].add(obj['Key'].split('/')[3]) # Filename only
                            
            # Get only the database names from the cluster
            cluster_db_names = set(db_info.keys())
            s3_databases = set(databases.keys())
            available_databases = []

            if dbname:
                # 1. Get values in S3 and cluster
                available_databases = list(s3_databases.intersection(cluster_db_names))

                # 2. Get values not in S3 but in cluster
                missing_in_databases = cluster_db_names.difference(s3_databases)
                if len(missing_in_databases) > 0:
                    logging.warning(f"No backup file exists for the database '{missing_in_databases}' with the specified timestamp: {timestamp}.")
                    print(Color.ORANGE + f"No backup file exists for the database '{missing_in_databases}' with the specified timestamp: {timestamp}." + Color.RESET)
            else:
                # Check each value in dbnames
                
                #1. Get values not in S3 but in cluster
                if dbname in databases and dbname in cluster_db_names: 
                    available_databases.append(dbname)

                #2. Get values not in cluster    
                elif dbname in databases: 
                    logging.warning(f"Database '{dbname}' does not exist in the cluster.")
                    print(Color.ORANGE + f"Database '{dbname}' does not exist in the cluster." + Color.RESET) 

                #3. Get values not in s3
                elif dbname in cluster_db_names:
                        logging.warning(f"No backup file exists for the database '{dbname}' with the specified timestamp: {timestamp}.")
                        print(Color.ORANGE + f"No backup file exists for the database '{dbname}' with the specified timestamp: {timestamp}." + Color.RESET)      
            
            # import operation       
            
            for db_name, files in databases.items():            
                if db_name in available_databases:
                    logging.info(f"Importing data from dbname:{db_name}...")
                    print(Color.INFO + f"Importing data from dbname:{db_name}..." + Color.RESET)
                    uid = db_info[db_name].get('uid')
                    import_data = []
                    for file in files:
                        if file:
                            import_data.append({
                                "type": "s3",
                                "bucket_name": bucketname,
                                "subdir": f"/{hostname}/{db_name}/{timestamp}/",
                                "filename": file,
                                "access_key_id": aws_access_key_id,
                                "secret_access_key": aws_secret_access_key
                            })
                    
                    data = {"dataset_import_sources": import_data, "email_notification": False}
                    try:
                        response = requests.post(
                            f"https://{hostname}:{port}/v1/bdbs/{uid}/actions/import",
                            auth=auth,
                            headers={"Content-Type": "application/json"},
                            json=data,
                            verify=False
                        )
                        response.raise_for_status()
                        # Check Redis logs for import sequence
                        if check_redis_log(
                                user=userid,
                                pwd=password,
                                url=f"https://{hostname}:{port}/v1/logs",
                                bdb_uid=uid,
                                bucket_name=bucketname,
                                region_name=f"{hostname}/{db_name}/{timestamp}",
                                operation=operation,
                                max_wait_minutes=15,
                                poll_interval_seconds=10,
                                verify_ssl=False
                            ):
                            logging.info(f"Verified Redis log: {operation.capitalize()} succeeded for database {db_name}")
                            print(Color.GREEN + f"Verified Redis log: {operation.capitalize()} succeeded for database {db_name}" + Color.RESET)
                            print(Color.GREEN + f"Redis {operation.capitalize()} completed successfully. See log: {log_file}" + Color.RESET)
                        else:
                            logging.error(f"Redis log verification failed or timed out for database {db_name}")
                            print(Color.RED + f"Redis log verification failed or timed out for database {db_name}" + Color.RESET)
                            print(Color.RED + f"{operation.capitalize()} did not complete. Check log for details: {log_file}" + Color.RESET)
                            
                    except Exception as e:
                        logging.error(f"{operation.capitalize()} failed for {db_name}: {e}")
                        print(Color.RED + f"{operation.capitalize()} failed for {db_name}: {e}" + Color.RESET)                    
    
if __name__ == "__main__":
    main()

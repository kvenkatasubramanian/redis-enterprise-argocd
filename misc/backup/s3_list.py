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
from datetime import datetime, timedelta, timezone
import urllib3
import re
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

# Delete all objects under a given prefix
def delete_objects(s3, bucketname, prefix):
    deleted_count = 0

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucketname, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                s3.delete_object(Bucket=bucketname, Key=obj['Key'])
                deleted_count += 1
                logging.info(f"Deleted S3 object: {bucketname}/{obj['Key']}")

    return deleted_count


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


def delete_s3_timestamp_folders(
    s3,
    bucketname: str,
    hostname: str,
    dbname: str,
    retention_days: int = 5
) -> dict:
    """
    Delete S3 folders named as timestamps (YYYYMMDDHHMMSS) older than retention_days.
    """

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)

    logging.info(
        f"Starting S3 cleanup before export: "
        f"s3://{bucketname}/{hostname}/{dbname}, "
        f"retention_days={retention_days}, cutoff={cutoff.isoformat()}"
    )

    paginator = s3.get_paginator("list_objects_v2")
    timestamps = set()

    for page in paginator.paginate(
        Bucket=bucketname,
        Prefix=f"{hostname}/{dbname}/"
    ):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            parts = obj["Key"].split("/")
            if len(parts) > 2:
                timestamps.add(parts[2])

    old_timestamps = []
    for ts in sorted(timestamps):
        try:
            ts_dt = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(
                tzinfo=timezone.utc
            )
            if ts_dt < cutoff:
                old_timestamps.append(ts)
        except ValueError:
            logging.warning(f"Skipping invalid timestamp folder: {ts}")

    total_objects_deleted = 0

    for ts in old_timestamps:
        prefix = f"{hostname}/{dbname}/{ts}/"
        logging.info(f"Deleting old backup folder: s3://{bucketname}/{prefix}")

        for page in paginator.paginate(
            Bucket=bucketname,
            Prefix=prefix
        ):
            if "Contents" in page:
                for obj in page["Contents"]:
                    s3.delete_object(
                        Bucket=bucketname,
                        Key=obj["Key"]
                    )
                    total_objects_deleted += 1
                    logging.info(
                        f"Deleted S3 object: "
                        f"s3://{bucketname}/{obj['Key']}"
                    )

    logging.info(
        f"S3 cleanup completed: folders_deleted={len(old_timestamps)}, "
        f"objects_deleted={total_objects_deleted}"
    )

    return {
        "folders_deleted": len(old_timestamps),
        "objects_deleted": total_objects_deleted,
        "folders": old_timestamps
    }


def main():
    parser = argparse.ArgumentParser(description='Export or Import databases to/from AWS S3')
    parser.add_argument("--mode", required=True, choices=["list", "import"], help="list or import mode")   
    parser.add_argument("--timestamp", help="Timestamp for import mode")

    args = None
    try:
        args = parser.parse_args()

    except argparse.ArgumentError as e:
        print("Error:", e.message)
        parser.print_help()
        sys.exit(1)

    mode = args.mode                                                                                        
    timestamp = args.timestamp 

    # Read secrets from environment variables (Kubernetes Secrets)
    hostname = os.getenv('REC_NAME')
    userid = os.getenv('REC_USERNAME')
    password = os.getenv('REC_PASSWORD')
    bucketname = os.getenv('BUCKET_NAME')
    port = 9443
    operation = 'import'
    dbname = os.getenv('DB_NAME')
    endpoint_url = os.getenv('ENDPOINT_URL')  
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key_id = os.getenv('AWS_SECRET_ACCESS_KEY_ID')


    missing = []
    if not hostname: missing.append("REC_NAME") 
    if not userid: missing.append("REC_USERNAME")
    if not password: missing.append("REC_PASSWORD")
    if not bucketname: missing.append("BUCKET_NAME")
    if not dbname: missing.append("DB_NAME")
    if not endpoint_url: missing.append("ENDPOINT_URL")       
    if not aws_access_key_id: missing.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key_id: missing.append("AWS_SECRET_ACCESS_KEY_ID")

    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        print(Color.RED + f"Missing required environment variables: {', '.join(missing)}" + Color.RESET)
        sys.exit(1)

    # Validate timestamp only for import mode
    if mode == "import" and not timestamp:                                                         
        print(Color.RED + "Error: --timestamp is required in import mode" + Color.RESET)
        logging.error("Error: --timestamp is required in import mode")
        sys.exit(1)

    # Get Database info
    url = f"https://{hostname}:{port}/v1/bdbs"
    auth = (userid, password)

    try:
        response = requests.get(url, auth=auth, verify=False)
        if response.status_code == 401:
            print(Color.RED + f"Error: Unauthorized: 401 error" + Color.RESET)
            sys.exit(1)
        data = json.loads(response.text)        
    except requests.exceptions.RequestException as e:
        print(Color.RED + f"Error: {e}" + Color.RESET)
        logging.error(f"Error: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(Color.RED + f"Failed to parse JSON response: {e}" + Color.RESET)
        logging.error(f"Failed to parse JSON response: {e}")
        sys.exit(1)

    # Extract uid and names only for the specified dbname
    db_info = {}
    for item in data:
        if item['name'] == dbname:          
            uid = item['uid']
            name = item['name']
            db_info[name] = get_db_info(hostname,port,uid, auth)
     
    
    # Check if any specified dbname do not exist in the cluster
    if dbname not in db_info:    
        print(Color.RED + f"Database '{dbname}' does not exist in the cluster." + Color.RESET)
        logging.error(f"Database '{dbname}' does not exist in the cluster.")
        sys.exit(1)


    # Connect to S3
    print(Color.INFO + f"Starting import operation for dbname:{dbname}" + Color.RESET)
    logging.info(f"Starting import operation for dbname:{dbname}")    
    if endpoint_url == '' or endpoint_url is None:
        s3 = boto3.client('s3', 
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key_id)

    else:
        #bofa specific endpoint
        s3 = boto3.client('s3', 
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key_id,
            verify=False)
            
    try:
        s3.head_bucket(Bucket=bucketname)            
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.error(f"Subdirectories {hostname} does not exist.")
            print(Color.RED + f'Subdirectories {hostname} does not exist.' + Color.RESET)
            sys.exit(1)
        else:
            raise        
    
    # Part I: Mode == list
    # List directories under the bucket
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucketname, Prefix=hostname)
    databases = {}
    timestamps = set()

    # Add all databases
    for page in result:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                parts = obj['Key'].split('/')
                if len(parts) > 2:
                    tmp_db_name = parts[1]   #db_name
                    if tmp_db_name == dbname:                            
                        timestamps.add(parts[2]) # timestamp

    #List all the timestamps for the specified dbname
    timestamps = sorted(timestamps) 
    if not timestamps:
        logging.warning(f"No timestamps found for given {dbname}. Import cannot proceed.")
        print(Color.RED +f"No timestamps found for given {dbname}. Import cannot proceed." + Color.RESET)
        sys.exit(1) 

    # -------------------------------
    # MODE = LIST
    # -------------------------------
    if mode == "list":                                                                                       
        print(Color.INFO + f"\nAvailable timestamps for the database: {dbname}" + Color.RESET)
        logging.info(f"Available timestamps for the database: {dbname}")
        for i, ts in enumerate(timestamps, 1):
                print(f"{i}. {ts}")

        summary = delete_s3_timestamp_folders(
            s3=s3,
            bucketname=bucketname,
            hostname=hostname,
            dbname=dbname,
            retention_days=5
        )

        print(Color.GREEN + f"\n {summary} " + Color.RESET)

    
if __name__ == "__main__":
    main()

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


def main():

    # Read secrets from environment variables (Kubernetes Secrets)
    hostname = os.getenv('REC_NAME')
    userid = os.getenv('REC_USERNAME')
    password = os.getenv('REC_PASSWORD')
    bucketname = os.getenv('BUCKET_NAME')
    port = 9443
    operation = 'export'
    dbname = os.getenv('DB_NAME')
    endpoint_url = os.getenv('ENDPOINT_URL')  
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key_id = os.getenv('AWS_SECRET_ACCESS_KEY_ID')

    missing = []
    if not hostname: missing.append("REC_NAME") 
    if not userid: missing.append("REC_USERNAME")
    if not password: missing.append("REC_PASSWORD")
    if not bucketname: missing.append("BUCKET_NAME")
    if not endpoint_url: missing.append("ENDPOINT_URL")       
    if not aws_access_key_id: missing.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key_id: missing.append("AWS_SECRET_ACCESS_KEY_ID")

    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        print(Color.RED + f"Missing required environment variables: {', '.join(missing)}" + Color.RESET)
        sys.exit(1)


    # Get Database info
    url = f"https://{hostname}:{port}/v1/bdbs"
    auth = (userid, password)

    db_info = {}

    try:
        response = requests.get(url, auth=auth, verify=False)
        if response.status_code == 401:
            print(Color.RED + f"Error: Unauthorized: 401 error" + Color.RESET)
            sys.exit(1)
        data = json.loads(response.text)        
    except requests.exceptions.RequestException as e:
        print(Color.RED + f"Error: {e}" + Color.RESET)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(Color.RED + f"Failed to parse JSON response: {e}" + Color.RESET)
        sys.exit(1)

    # Extract uid and names only for the specified dbname
    for item in data:
        uid = item['uid']
        name = item['name']

        # If dbname is set, skip non-matching DBs
        if dbname and name != dbname:
            continue

        db_info[name] = {'uid': uid, 'no_of_keys': 1}
        db_info[name]["persistence"] = (item.get("data_persistence") == "aof")


    if dbname and dbname not in db_info:
        print(Color.INFO + f"Database '{dbname}' does not exist in the cluster." + Color.RESET)

    # Export operation 
    for db_name, db_details in db_info.items():
        persistence = db_details['persistence']
        print(Color.INFO + f"Processing Database: {db_name}, Persistence: {persistence} - Backup can't be done!" + Color.RESET)
        if persistence:
            uid = db_details['uid']
            no_of_keys = db_details['no_of_keys']
            subdir = now_folder()               

            logging.info(f"Database: {db_name}, total_keys: {no_of_keys}")
            print(Color.INFO + f"Database: {db_name}, total_keys: {no_of_keys}" + Color.RESET)

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


            prefix = f"{hostname}/{db_name}/{subdir}/"

            try:
                # Check if any objects exist under the prefix
                response = s3.list_objects_v2(
                    Bucket=bucketname,
                    Prefix=prefix,
                    MaxKeys=1
                )

                if response.get("KeyCount", 0) > 0:
                    logging.info(f"S3 path exists. Deleting: {bucketname}/{prefix}")
                    print(Color.INFO + f"Deleting existing S3 path: {bucketname}/{prefix}" + Color.RESET)

                    delete_objects(s3, bucketname, prefix)

                # Recreate folder marker (optional but safe)
                s3.put_object(Bucket=bucketname, Key=prefix)

            except botocore.exceptions.ClientError as e:
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
                    "secret_access_key": aws_secret_access_key_id   
                }, 
                "email_notification": True
            }
        
            response = requests.post(
                    f"https://{hostname}:{port}/v1/bdbs/{uid}/actions/export",
                    auth=auth,
                    headers={'Content-Type': 'application/json'},
                    json=export_data,
                    verify=False
                    )
            if response.status_code == 200:
                logging.info(f"Redis {operation.capitalize()} completed successfully. See log: {log_file}")
                print(Color.GREEN + f"Redis {operation.capitalize()} completed successfully. See log: {log_file}" + Color.RESET)
            else:
                logging.error(f"Redis {operation.capitalize()} failed!, response.text: {response.text}")
                print(Color.RED + f"{operation.capitalize()} did not complete. Check log for details: {log_file}" + Color.RESET)


if __name__ == "__main__":
    main()

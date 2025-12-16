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
    if not dbname: missing.append("DB_NAME")
    if not endpoint_url: missing.append("ENDPOINT_URL")       
    if not aws_access_key_id: missing.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key_id: missing.append("AWS_SECRET_ACCESS_KEY_ID")

    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        print(Color.RED + f"Missing required environment variables: {', '.join(missing)}" + Color.RESET)
        sys.exit(1)

    timestamp = ''

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
        if item['name'] == dbname:          
            uid = item['uid']
            name = item['name']
            db_info[name] = get_db_info(hostname,port,uid, auth)
     
    
    # Check if any specified dbname do not exist in the cluster
    if dbname not in db_info:    
        print(Color.ORANGE + f"Database '{dbname}' does not exist in the cluster." + Color.RESET)

    if operation == 'import':
        print(Color.INFO + f"Starting import operation for dbname:{dbname}" + Color.RESET)
        logging.info(f"Starting import operation for dbname:{dbname}")

        endpoint_url = args.endpoint_url
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
                    
                    tmp_db_name = key.split('/')[1]    #db_name
                    tmp_timestamp = key.split('/')[2]  #timestamp
                    timestamps.add(tmp_timestamp)

        #List all the timestamps for the specified dbname
        timestamps = sorted(timestamps) 
        if not timestamps:
            logging.warning(f"No timestamps found for given {dbname}. Import cannot proceed.")
            print(Color.RED +f"No timestamps found for given {dbname}. Import cannot proceed." + Color.RESET)
            sys.exit(1) 
        
        print(f"\nAvailable timestamps for the database: {dbname}")
        for i, ts in enumerate(timestamps, 1):
                print(f"{i}. {ts}")

        # Ask user to select one
        while True:
            choice = input("\nSelect a timestamp by number (0 to exit): ")
            if choice == "0":
                print(Color.RED +"Exiting." + Color.RESET)
                sys.exit(0)
            if choice.isdigit() and 1 <= int(choice) <= len(timestamps):
                timestamp = timestamps[int(choice) - 1]
                logging.info(f"You selected: {timestamp}")
                print(Color.GREEN + f"\nYou selected: {timestamp}" + Color.RESET)
                break
            else:
                print(Color.ORANGE + f"Invalid choice. Try again." + Color.RESET)

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
                
                data = {"dataset_import_sources": import_data, "email_notification": True}
                
                response = requests.post(
                    f"https://{hostname}:{port}/v1/bdbs/{uid}/actions/import",
                    auth=auth,
                    headers={"Content-Type": "application/json"},
                    json=data,
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

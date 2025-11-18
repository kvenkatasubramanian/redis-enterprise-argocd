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


def main():
    parser = argparse.ArgumentParser(description='Export databases to/from AWS S3 using input.csv')
    parser.add_argument('-csv', required=True, help='Path to input CSV file with parameters')

    args = parser.parse_args()
    csv_file = args.csv

    # Check if the file exists
    if not os.path.isfile(csv_file):
        print(Color.RED + f"ERROR: CSV file '{csv_file}' does not exist." + Color.RESET)
        parser.print_help()
        sys.exit(1)

    params_list = read_csv_parameters(csv_file)    


    # Read secrets from environment variables (Kubernetes Secrets)
    # db_user = os.getenv('DB_USER')
    # db_password = os.getenv('DB_PASSWORD')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    missing = []
    # if not db_user: missing.append("DB_USER")
    # if not db_password: missing.append("DB_PASSWORD")
    if not aws_access_key_id: missing.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key: missing.append("AWS_SECRET_ACCESS_KEY")

    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        print(Color.RED + f"Missing required environment variables: {', '.join(missing)}" + Color.RESET)
        sys.exit(1)


    for params in params_list:
        # Extract the parameters
        hostname = params['hostname']
        userid = params['user']
        password = params['password']
        bucketname = params['bucket']
        port = params['port']
        operation = params['operation']
        dbname = params['dbname']
        endpoint_url = params['endpoint_url']

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


        # Export operation
        if operation == 'export':
            for db_name, db_details in db_info.items():
                uid = db_details['uid']
                no_of_keys = db_details['no_of_keys']
                subdir = now_folder()               

                logging.info(f"Database: {db_name}, total_keys: {no_of_keys}")
                print(Color.INFO + f"Database: {db_name}, total_keys: {no_of_keys}" + Color.RESET)

                if endpoint_url == '' or endpoint_url is None:
                    s3 = boto3.client('s3', 
                        aws_access_key_id=aws_access_key_id, 
                        aws_secret_access_key=aws_secret_access_key)
                else:
                    #bofa specific endpoint
                    s3 = boto3.client('s3', 
                        endpoint_url=endpoint_url,
                        aws_access_key_id=aws_access_key_id, 
                        aws_secret_access_key=aws_secret_access_key,
                        verify=False)

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

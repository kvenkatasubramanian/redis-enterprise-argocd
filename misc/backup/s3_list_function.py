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


def main():
    cluster_name = os.getenv('CLUSTER_NAME')
    bucketname = os.getenv('BUCKET_NAME')
    endpoint_url = os.getenv('ENDPOINT_URL')  
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key_id = os.getenv('AWS_SECRET_ACCESS_KEY_ID')


    missing = []
    if not cluster_name: missing.append('CLUSTER_NAME')
    if not bucketname: missing.append("BUCKET_NAME")
    if not endpoint_url: missing.append("ENDPOINT_URL")       
    if not aws_access_key_id: missing.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key_id: missing.append("AWS_SECRET_ACCESS_KEY_ID")

    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        print(Color.RED + f"Missing required environment variables: {', '.join(missing)}" + Color.RESET)
        sys.exit(1)


    # Connect to S3
    s3 = boto3.client('s3', 
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key_id,
        verify=False)
            
    try:
        s3.head_bucket(Bucket=bucketname)            
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.error(f"Subdirectories {cluster_name} does not exist.")
            print(Color.RED + f'Subdirectories {cluster_name} does not exist.' + Color.RESET)
            sys.exit(1)
        else:
            raise        
    
   # List directories under the bucket
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucketname, Prefix=cluster_name)
    timestamps = set()

    # Add all databases
    for page in result:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                parts = obj['Key'].split('/')
                if len(parts) > 2:
                    tmp_db_name = parts[1]   #db_name
                    timestamps.add(tmp_db_name, parts[2]) # timestamp

    #List all the timestamps for the specified dbname
    timestamps = sorted(timestamps) 
    if not timestamps:
        logging.warning(f"No timestamps found for given {dbname}. Import cannot proceed.")
        print(Color.RED +f"No timestamps found for given {dbname}. Import cannot proceed." + Color.RESET)
        sys.exit(1) 

    print(Color.INFO + f"\nAvailable timestamps for the cluster: {cluster_name}" + Color.RESET)
    logging.info(f"Available timestamps for the cluster: {cluster_name}")
    for i, ts in enumerate(timestamps, 1):
            print(f"{i}. {ts}")


    
if __name__ == "__main__":
    main()

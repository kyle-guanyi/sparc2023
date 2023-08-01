import json
import os
import pymysql
import subprocess
import boto3
import datetime

import sys
import logging

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # You can set the logging level as per your requirements


def lambda_handler(event, context):
    # Database connection info
    db_host = "sparc-db.c56gikbu12n4.us-east-1.rds.amazonaws.com"
    db_user = "admin"
    db_password = "sparc2023"
    db_name = "SPARC_DB"
    db_port = 3306

    # S3 bucket info
    s3_bucket = "kevin-testbucket-sparc"

    # Extract the s3_folder value from the event dictionary
    s3_folder = event.get('s3_folder')  # Replace 's3_folder' with the actual key name you expect in the event

    # Generate the filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{db_name}_snapshot_{timestamp}.sql"

    # MySQL dump command
    dump_command = f"mysqldump -h {db_host} -u {db_user} -p{db_password} {db_name}"

    # Execute the MySQL dump command and upload to S3
    try:
        with subprocess.Popen(dump_command, stdout=subprocess.PIPE, shell=True) as process:
            s3_key = os.path.join(s3_folder, filename) if s3_folder else filename
            s3 = boto3.client("s3")
            logger.info("Database snapshot:  " + filename + " is being copied to new bucket " + s3_bucket)
            s3.upload_fileobj(process.stdout, s3_bucket, s3_key)
        return {"statusCode": 200, "body": f"Snapshot successfully uploaded to S3: {filename}"}
    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

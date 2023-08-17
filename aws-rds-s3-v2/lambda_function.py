import boto3
import datetime

def lambda_handler(event, context):
    # Database info
    db_arn = "arn:aws:rds:us-east-1:740995473525:db:sparc-db"
    db_name = "SPARC_DB"

    # S3 bucket info
    s3_bucket = "kevin-testbucket-sparc"

    # Generate the filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{db_name}_snapshot_{timestamp}.sql"

    # Create the RDS Data API client
    rds_data = boto3.client('rds-data')

    # Start the export task
    try:
        response = rds_data.start_export_task(
            exportTaskIdentifier=f"{db_name}-export-{timestamp}",
            sourceArn=db_arn,
            s3BucketName=s3_bucket,
            s3Prefix=s3_folder,
            exportOnly=[{
                'databaseName': db_name,
                'tableName': 'your_table_name',  # Optional: If you want to export specific tables
            }],
        )
        return {"statusCode": 200, "body": f"Export task started: {response['exportTaskIdentifier']}"}
    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
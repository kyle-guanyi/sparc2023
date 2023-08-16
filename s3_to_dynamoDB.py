import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import re


dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')

"""
    Create a DynamoDB using given table name
"""
def create_dynamodb_table(table_name, region):
    try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        
        table = dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions = [
                {
                    'AttributeName': 'pk',
                    'AttributeType': 'N'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'pk',
                    'KeyType': 'HASH'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 50
            }
        )
        table.wait_until_exists()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

"""
    Read compressed data from S3 and export it to Data Frame
    bucket_name is the S3 bucket name
    object_name is the folder/object name of for corresponding snapshot
"""
def export_s3_to_dataframe(bucket_name, object_name):

    print(f"Exporting S3 backup: {object_name}")

    try:
        
        # Get the data from the S3 bucket
        bucket = s3_client.list_objects_v2(Bucket=bucket_name)

        dataSource = None
        for obj in bucket['Contents']:
            key = obj['Key']
            if object_name in key and ".gz.parquet" in key:
                #print(obj)
                dataSource = s3_client.get_object(Bucket=bucket_name, Key=key)
                break

        buffer = io.BytesIO(dataSource ['Body'].read())

        # Read the Parquet file from the buffer using pyarrow
        parquet_table = pq.read_table(buffer)

        # Convert the pyarrow table to a pandas DataFrame
        df = parquet_table.to_pandas()

        # Now you can work with the DataFrame 'df'
        print(df.head())
        print(len(df.index))
        print("\n")
        
        print("Export to DataFrame completed successfully.")
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    

"""
    Store data in DataFrame to DynamoDB
"""
def dataframe_to_dynamoDB(df, dynamodb_table_name):
    if df is None:
        print("No DataFrame is saved from S3.")
        return 1

    # Get the DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(dynamodb_table_name)

    # Store data in the DynamoDB table in batch
    with table.batch_writer() as batch:
        for i, row in df.iterrows():
            batch.put_item(Item=row.to_dict())

    print("Export to DynamoDB completed successfully.")
    return 0


"""
    Retrieve 10 items (in random order) from DynamoDB
"""
def retrieve_from_dynamoDB(dynamodb_table_name):
    
    response = dynamodb_client.describe_table(
    TableName=dynamodb_table_name
    )
    print(response, "\n")


    table = dynamodb.Table(dynamodb_table_name)

    response = table.scan(Limit=10)
    print(response["Items"], "\n")


"""
    Return backup folder names in given S3 bucket
    folder names are in ascending order, based on created time
"""
def get_s3_backups_date_range(bucket_name):
    result = s3_client.list_objects(Bucket=bucket_name, Prefix='sparc-export', Delimiter='/')
    folder_names = []
    for o in result.get('CommonPrefixes'):
        folder_names.append(o.get('Prefix'))
    return folder_names


"""
    Print the first 3 folders and last 3 folders in given folder_names list
"""
def print_s3_backups_date_range(folder_names):
    print("S3 backups date range shown below (backup named as YYYY-MM-DD-HH-MM-SS):")
    length = len(folder_names)
    for i in range(3):
        if i < length:
            print(folder_names[i])
    if length > 3:
        for i in range(3):
            print(".")
        for i in range(length - 3, length):
            if i > 2:
                print(folder_names[i])

    print("\n")


"""
    Get user input of the date and time of the S3 backup,
    find the full name of the folder in folder_names
    and return the full name.
"""
def get_user_input_datetime(folder_names):

    val = input("Enter the date and time of the S3 backup (in format of YYYY-MM-DD-HH): ")

    while not re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}", val):
        print("Error: input format is invalid! Please try again!\n")
        val = input("Enter the date and time of the S3 backup (in format of YYYY-MM-DD-HH): ")
    
    print("\n")
    res = None
    
    for f in folder_names:
        if val in f:
            res = f

    return res


if __name__ == "__main__":

    print("Welcome! You can choose which S3 backup object to read into DynamoDB here!")
    
    region = "us-west-2"
    s3_bucket_name = "sparc-db-cross-region-replication-bucket-us-west-2"

    s3_folders = get_s3_backups_date_range(s3_bucket_name)
    print_s3_backups_date_range(s3_folders)
    object_name = get_user_input_datetime(s3_folders)

    while not object_name:
        print("Error: invalid date time. Please see the date range of S3 backups below and try again!")
        print("\n")
        print_s3_backups_date_range(s3_folders)
        object_name = get_user_input_datetime(s3_folders)
    
    dynamodb_table_name = object_name[:-1]  # exclude "/"
    df = export_s3_to_dataframe(s3_bucket_name, object_name)
    create_dynamodb_table(dynamodb_table_name, region)
    dataframe_to_dynamoDB(df, dynamodb_table_name)
    #retrieve_from_dynamoDB(dynamodb_table_name, region)



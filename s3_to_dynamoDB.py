import boto3
import pandas as pd
import pyarrow.parquet as pq
import io


"""
    Create a DynamoDB using given table nam\
"""
def create_dynamodb_table(table_name):
    try:
        dynamodb = boto3.resource('dynamodb')

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
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

"""
    Read compressed data from S3 and export it to Data Frame
"""
def export_s3_to_dataframe(bucket_name):
    try:
        s3_client = boto3.client('s3')
        # Create a new DynamoDB table
        #create_dynamodb_table(dynamodb_table_name)

        # Get the data from the S3 bucket
        bucket = s3_client.list_objects_v2(Bucket=bucket_name)

        dataSource = None
        for obj in bucket['Contents']:
            key = obj['Key']
            if ".gz.parquet" in key:
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
    
    # Convert the DataFrame to a list of dictionaries
    data_list = df.to_dict(orient='records')

    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb')

    # Get the DynamoDB table
    table = dynamodb.Table(dynamodb_table_name)

    # Store each item (dictionary) in the DynamoDB table
    count = 0
    for item in data_list:
        table.put_item(Item=item)

    print("Export to DynamoDB completed successfully.")
    return 0

if __name__ == "__main__":
    s3_bucket_name = "testsharon1"
    dynamodb_table_name = "testdynamo"
    df = export_s3_to_dataframe(s3_bucket_name)
    create_dynamodb_table(dynamodb_table_name)
    dataframe_to_dynamoDB(df, dynamodb_table_name)

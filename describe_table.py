import boto3
from botocore.exceptions import ClientError

# Configuration
REGION_NAME = 'eu-central-1'  # e.g., 'us-west-2'
TABLE_NAME = 'interconnecthub_workflows'  # e.g., 'example-table'

def describe_dynamodb_table(region_name: str, table_name: str) -> None:
    try:
        # Initialize a session using Amazon DynamoDB
        dynamodb_client = boto3.client('dynamodb', region_name=region_name)
        
        # Describe the specified DynamoDB table
        response = dynamodb_client.describe_table(TableName=table_name)
        
        print('response: ', response)
        # Output the table details
        print("Table Name:", response['Table']['TableName'])
        print("Table Status:", response['Table']['TableStatus'])
        print("Table Item Count:", response['Table']['ItemCount'])
        print("Table Creation Date:", response['Table']['CreationDateTime'])
        print("Table Provisioned Throughput (Read Capacity Units):", response['Table']['ProvisionedThroughput']['ReadCapacityUnits'])
        print("Table Provisioned Throughput (Write Capacity Units):", response['Table']['ProvisionedThroughput']['WriteCapacityUnits'])
        print("Table Key Schema:", response['Table']['KeySchema'])
        print("Table Attribute Definitions:", response['Table']['AttributeDefinitions'])
        
    except ClientError as e:
        print(f"Failed to describe table. Table name: {table_name}")
        print(e)

if __name__ == '__main__':
    describe_dynamodb_table(REGION_NAME, TABLE_NAME)

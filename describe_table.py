import boto3

def describe_table(table_name):
    # Initialize the DynamoDB client for local instance
    dynamodb = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

    try:
        # Call the describe_table method
        response = dynamodb.describe_table(TableName=table_name)
        return response['Table']
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"Table {table_name} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    table_name = 'customer_table_info'  # Replace with your table name
    table_description = describe_table('ecommerce-suppliers')
    if table_description:
        print(table_description)

import boto3
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import List
from datetime import datetime

# Configuration
REGION_NAME = 'eu-central-1'  # e.g., 'us-west-2'
TABLE_NAME = 'interconnecthub_workflows_executions'  # e.g., 'example-table'

@dataclass
class BackupDetail:
    name: str
    status: str
    creation_time: str
    type: str
    size: int

class DynamoDBBackupFetcher:
    def __init__(self, region_name: str) -> None:
        self.region_name = region_name
        self.dynamodb_client = self.__configure_dynamodb_client()

    def __configure_dynamodb_client(self) -> boto3.client:
        return boto3.client('dynamodb', region_name=self.region_name)

    def get_dynamoDB_table_backup_details(self, table_name: str) -> List[BackupDetail]:
        try:
            response = self.dynamodb_client.describe_backup(BackupArn="arn:aws:backup:eu-central-1:451445658243:backup-vault:Default")
            print(response)
            backup_details = [
                BackupDetail(
                    name=backup_summary["BackupName"],
                    status=backup_summary["BackupStatus"],
                    creation_time=backup_summary["BackupCreationDateTime"],
                    type=backup_summary["BackupType"],
                    size=backup_summary["BackupSizeBytes"]
                )
                for backup_summary in response.get("BackupSummaries", [])
            ]
            return backup_details
        except ClientError as e:
            print(f"Failed to retrieve backup details of DynamoDB table. table_name: {table_name}")
            print(e)
            return []

if __name__ == '__main__':
    fetcher = DynamoDBBackupFetcher(REGION_NAME)
    backup_details = fetcher.get_dynamoDB_table_backup_details(TABLE_NAME)
    
    for backup in backup_details:
        print(f"Name: {backup.name}, Status: {backup.status}, Creation Time: {backup.creation_time}, Type: {backup.type}, Size: {backup.size}")

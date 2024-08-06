import boto3
from botocore.exceptions import ClientError
from dacite import from_dict
from dataclasses import dataclass
from typing import List

# Configuration
REGION_NAME = 'eu-central-1'  # e.g., 'us-west-2'

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
        self.backup_client = self.__configure_backup_client()

    def __configure_backup_client(self) -> boto3.client:
        return boto3.client('backup', region_name=self.region_name)

    def get_dynamoDB_table_backup_details(self) -> List[BackupDetail]:
        try:
            response = self.backup_client.list_backup_jobs(ByResourceArn="arn:aws:dynamodb:eu-central-1:451445658243:table/interconnecthub_workflows")
            print('length: ', len(response['BackupJobs']))
            for backupJob in response['BackupJobs']:
                print("----------------------------------")
                print('backupjob: ', backupJob)
        except ClientError as e:
            print(f"Failed to retrieve backup details of DynamoDB table.")
            print(e)
            return []

if __name__ == '__main__':
    fetcher = DynamoDBBackupFetcher(REGION_NAME)
    fetcher.get_dynamoDB_table_backup_details()
    

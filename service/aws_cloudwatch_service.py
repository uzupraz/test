import boto3
import json

from botocore.exceptions import ClientError
from typing import List, Tuple

from utils import Singleton
from  configuration import AWSConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class AWSCloudWatchService(metaclass=Singleton):


    def __init__(self, aws_config: AWSConfig) -> None:
        """
        Initializes the service with the provided AWS configuration.

        Args:
            aws_config (AWSConfig): The configuration object containing AWS-related details such as ARNs and execution roles.
        """
        self.aws_config = aws_config
        self.cloudwatch_client = boto3.client('logs')


    def create_log_group(self, log_group_name: str):
        log.info("Creating log group. log_group_name: %s", log_group_name)
        try:
            self.cloudwatch_client.create_log_group(logGroupName=log_group_name)
            log.info("Log group created. log_group_name: %s", log_group_name)

            # Getting group arn
            response = self.cloudwatch_client.describe_log_groups(logGroupNamePrefix=log_group_name)
            log_group = response.get('logGroups', [{}])[0]
            arn = log_group.get('arn', None)

            if not arn:
                log.error("Unable to get log group arn. log_group_name: %s", log_group_name)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Unable to get log group')
            
            return arn
        except ClientError as e:
            log.exception("Failed to create log group. log_group_name: %s", log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to create log group')
        

    def update_retention_policy(self, log_group_name: str, retention_in_days: int) -> bool:
        log.info("Updating log group retention policy. log_group_name: %s", log_group_name)
        try:
            self.cloudwatch_client.put_retention_policy(logGroupName=log_group_name, retentionInDays=retention_in_days)
            log.info("Retention policy updated. log_group_name: %s", log_group_name)
        except ClientError as e:
            log.exception("Failed to update log group retention policy. log_group_name: %s", log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to update log group retention policy')
    
    
    def get_logging_configuration(self, log_group_arn: str, level: str = "ALL", include_execution_date: bool = True):
        return {
            "level": level,
            "includeExecutionData": include_execution_date,
            "destinations": [
                {
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": log_group_arn
                    }
                }
            ]
        }
    

    def does_log_group_exist(self, log_group_name: str) -> bool:
        log.info("Checking log group existance. log_group_name: %s", log_group_name)
        try:
            response = self.cloudwatch_client.describe_log_groups(logGroupNamePrefix=log_group_name)
            if any(group['logGroupName'] == log_group_name for group in response['logGroups']):
                log.info("Log group exists. log_group_name: %s", log_group_name)
                return True
            
            log.info("Log group does not exists. log_group_name: %s", log_group_name)
            return False
        except ClientError as e:
            log.exception("Failed to check log group existance. log_group_name: %s", log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to check log group existance')

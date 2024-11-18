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


    def __init__(self) -> None:
        """
        Initializes the service with the provided AWS configuration.
        """
        self.cloudwatch_client = boto3.client('logs')


    def create_log_group(self, log_group_name: str) -> str:
        """
        Creates a new CloudWatch log group with the specified name, retrieves its ARN, and returns it.

        Args:
            log_group_name (str): The name of the log group to be created.

        Returns:
            str: The ARN of the created log group.

        Raises:
            ServiceException: If the log group cannot be created or if the ARN cannot be retrieved.
        """
        log.info("Creating log group. log_group_name: %s", log_group_name)
        try:
            self.cloudwatch_client.create_log_group(logGroupName=log_group_name)
            log.info("Log group created. log_group_name: %s", log_group_name)

            # Getting group arn
            response = self.cloudwatch_client.describe_log_groups(logGroupNamePrefix=log_group_name)
            log_group = response.get('logGroups', [{}])
            # Check if log group is missing or doesn't have an ARN
            if not log_group or not log_group[0].get('arn'):
                log.error("Unable to create log group. log_group_name: %s", log_group_name)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Unable to create log group')

            return log_group[0].get('arn')
        except ClientError as e:
            log.exception("Failed to create log group. log_group_name: %s", log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to create log group')
        

    def update_retention_policy(self, log_group_name: str, retention_in_days: int) -> None:
        """
        Updates the retention policy for the specified log group in CloudWatch.

        Args:
            log_group_name (str): The name of the log group.
            retention_in_days (int): Number of days to retain log data.

        Raises:
            ServiceException: If updating the retention policy fails.
        """
        log.info("Updating log group retention policy. log_group_name: %s", log_group_name)
        try:
            self.cloudwatch_client.put_retention_policy(logGroupName=log_group_name, retentionInDays=retention_in_days)
            log.info("Retention policy updated. log_group_name: %s", log_group_name)
        except ClientError as e:
            log.exception("Failed to update log group retention policy. log_group_name: %s", log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to update log group retention policy')
    
    
    def get_logging_configuration(self, log_group_arn: str, level: str = "ALL", include_execution_date: bool = True) -> dict:
        """
        Constructs the logging configuration with specified log level and execution data inclusion.

        Args:
            log_group_arn (str): ARN of the CloudWatch log group for logging configuration.
            level (str): Log level (default: "ALL").
            include_execution_date (bool): Whether to include execution data (default: True).

        Returns:
            dict: Configuration for logging including log level and destinations.
        """
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
        """
        Checks if a CloudWatch log group with the specified name exists.

        Args:
            log_group_name (str): The name of the log group.

        Returns:
            bool: True if the log group exists, False otherwise.

        Raises:
            ServiceException: If an error occurs during the check.
        """
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

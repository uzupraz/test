import boto3
import json

from botocore.exceptions import ClientError
from typing import Optional

from utils import Singleton
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
            arn = self.get_log_group_arn(log_group_name)
            if not arn:
                log.error("Unable to create log group. log_group_name: %s", log_group_name)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Unable to create log group')

            return arn
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
    

    def get_log_group_arn(self, log_group_name: str) -> Optional[str]:
        """
        Returns log group arns for the provided log group name

        Args:
            log_group_name (str): The name of the log group.

        Returns:
            arn: True if the log group exists, False otherwise.

        Raises:
            ServiceException: If an error occurs during the process.
        """
        log.info("Getting log group arn. log_group_name: %s", log_group_name)
        try:
            response = self.cloudwatch_client.describe_log_groups(logGroupNamePrefix=log_group_name)
            log_group = response.get('logGroups', [])

            if not log_group:
                log.error("Log group not found. log_group_name: %s", log_group_name)
                return None
            
            return log_group[0].get('arn')
        except ClientError as e:
            log.exception("Failed to get log group arn. log_group_name: %s", log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to get log group arn')

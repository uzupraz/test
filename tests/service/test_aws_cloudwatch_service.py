import json

import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from utils import Singleton
from enums import ServiceStatus
from exception import ServiceException
from model import Workflow
from service import AWSCloudWatchService
from tests import TestUtils


class TestAWSCloudWatchService(unittest.TestCase):


    @patch('service.aws_cloudwatch_service.boto3.client')
    def setUp(self, mock_boto3_client) -> None:
        self.cloudwatch_client = MagicMock()
        mock_boto3_client.return_value = self.cloudwatch_client
        Singleton.clear_instance(AWSCloudWatchService)
        self.aws_cloudwatch_service = AWSCloudWatchService()


    def tearDown(self) -> None:
        self.aws_cloudwatch_service = None
        Singleton.clear_instance(AWSCloudWatchService)

    
    def test_create_log_group_success(self):
        """Test that a log group is created successfully with the correct ARN returned."""
        log_group_name = "test_log_group_name"
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group = MagicMock()

        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock(return_value = {
            "logGroups": [{"arn": "arn:aws:logs:region:account-id:log_group_name"}]
        })

        result = self.aws_cloudwatch_service.create_log_group(log_group_name)

        self.assertEqual(
            result,
            "arn:aws:logs:region:account-id:log_group_name"
        )
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group.assert_called_once_with(
            logGroupName=log_group_name
        )
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )


    def test_create_log_group_failure(self):
        """Test that a create log group throws exception."""
        log_group_name = "test_log_group_name"
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group = MagicMock()
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group.side_effect = ClientError(error_response, "create_log_group")

        with self.assertRaises(ServiceException) as context:
            self.aws_cloudwatch_service.create_log_group(log_group_name)

        self.assertEqual(context.exception.message, "Failed to create log group")
        self.assertEqual(context.exception.status_code, 500)
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group.assert_called_once_with(
            logGroupName=log_group_name
        )


    def test_describe_log_groups_returns_empty_list_should_throw_exception(self):
        """Test that a describe log groups returns empty group & raises exception"""
        log_group_name = "test_log_group_name"
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group = MagicMock()

        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock(return_value = {
            "logGroups": []
        })

        with self.assertRaises(ServiceException) as context:
            self.aws_cloudwatch_service.create_log_group(log_group_name)

        self.assertEqual(context.exception.message, "Unable to create log group")
        self.assertEqual(context.exception.status_code, 400)
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group.assert_called_once_with(
            logGroupName=log_group_name
        )


    def test_update_retention_policy_success(self):
        """Test that the retention policy is updated successfully."""
        log_group_name = "test_log_group_name"
        retention_in_days = 30
        self.aws_cloudwatch_service.cloudwatch_client.put_retention_policy = MagicMock()

        self.aws_cloudwatch_service.update_retention_policy(log_group_name, retention_in_days)

        self.aws_cloudwatch_service.cloudwatch_client.put_retention_policy.assert_called_once_with(
            logGroupName=log_group_name,
            retentionInDays=retention_in_days
        )


    def test_update_retention_policy_failure(self):
        """Test that a failure in updating the retention policy results in a ServiceException."""
        log_group_name = "test_log_group_name"
        retention_in_days = 30
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.aws_cloudwatch_service.cloudwatch_client.put_retention_policy.side_effect = ClientError(error_response, "put_retention_policy")

        with self.assertRaises(ServiceException) as context:
            self.aws_cloudwatch_service.update_retention_policy(log_group_name, retention_in_days)

        self.assertEqual(context.exception.message, "Failed to update log group retention policy")
        self.assertEqual(context.exception.status_code, 500)
        self.aws_cloudwatch_service.cloudwatch_client.put_retention_policy.assert_called_once_with(
            logGroupName=log_group_name,
            retentionInDays=retention_in_days
        )


    def test_get_logging_configuration(self):
        """Test that the logging configuration is returned correctly."""
        log_group_arn = "arn:aws:logs:region:account-id:log_group_name"
        expected_config = {
            "level": "ALL",
            "includeExecutionData": True,
            "destinations": [
                {
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": log_group_arn
                    }
                }
            ]
        }

        result = self.aws_cloudwatch_service.get_logging_configuration(log_group_arn)

        self.assertEqual(result, expected_config)


    def test_get_logging_configuration_with_custom_level(self):
        """Test that the logging configuration is returned with a custom level."""
        log_group_arn = "arn:aws:logs:region:account-id:log_group_name"
        expected_config = {
            "level": "ERROR",
            "includeExecutionData": True,
            "destinations": [
                {
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": log_group_arn
                    }
                }
            ]
        }

        result = self.aws_cloudwatch_service.get_logging_configuration(log_group_arn, level="ERROR")

        self.assertEqual(result, expected_config)


    def test_does_log_group_exist_true(self):
        """Test that the method returns True if the log group exists."""
        log_group_name = "test_log_group_name"
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock(return_value={
            "logGroups": [{"logGroupName": log_group_name}]
        })

        result = self.aws_cloudwatch_service.does_log_group_exist(log_group_name)

        self.assertTrue(result)
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )


    def test_does_log_group_exist_false(self):
        """Test that the method returns False if the log group does not exist."""
        log_group_name = "test_log_group_name"
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock(return_value={
            "logGroups": []
        })

        result = self.aws_cloudwatch_service.does_log_group_exist(log_group_name)

        self.assertFalse(result)
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )


    def test_does_log_group_exist_failure(self):
        """Test that a failure in checking log group existence results in a ServiceException."""
        log_group_name = "test_log_group_name"
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.side_effect = ClientError(error_response, "describe_log_groups")

        with self.assertRaises(ServiceException) as context:
            self.aws_cloudwatch_service.does_log_group_exist(log_group_name)

        self.assertEqual(context.exception.message, "Failed to check log group existance")
        self.assertEqual(context.exception.status_code, 500)
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )
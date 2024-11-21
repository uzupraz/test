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


    def test_create_log_group_fails_when_describe_log_group_throws_exception(self):
        """Test that create log groups fails due to exception in describe log group."""
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
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock()
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.side_effect = ClientError(error_response, "describe_log_groups")

        with self.assertRaises(ServiceException) as context:
            self.aws_cloudwatch_service.create_log_group(log_group_name)

        self.assertEqual(context.exception.message, "Failed to get log group arn")
        self.assertEqual(context.exception.status_code, 500)
        self.aws_cloudwatch_service.cloudwatch_client.create_log_group.assert_called_once_with(
            logGroupName=log_group_name
        )
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )


    def test_create_log_group_failes_when_describe_log_groups_returns_empty_list_should_throw_exception(self):
        """Test that create log group fails when describe log groups returns empty group & raises exception"""
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


    def test_get_log_group_arn(self):
        """Test that the method returns arn if the log group exists."""
        log_group_name = "test_log_group_name"
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock(return_value={
            "logGroups": [{"logGroupName": log_group_name, "arn": "test_arn"}]
        })

        result = self.aws_cloudwatch_service.get_log_group_arn(log_group_name)

        self.assertEqual(result, "test_arn")
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )


    def test_get_log_group_returns_none(self):
        """Test that the method returns None if the log group does not exist."""
        log_group_name = "test_log_group_name"
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups = MagicMock(return_value={
            "logGroups": []
        })

        result = self.aws_cloudwatch_service.get_log_group_arn(log_group_name)

        self.assertIsNone(result)
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )


    def test_get_log_group_arn_failure(self):
        """Test that a failure in getting log group arn results in a ServiceException."""
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
            self.aws_cloudwatch_service.get_log_group_arn(log_group_name)

        self.assertEqual(context.exception.message, "Failed to get log group arn")
        self.assertEqual(context.exception.status_code, 500)
        self.aws_cloudwatch_service.cloudwatch_client.describe_log_groups.assert_called_once_with(
            logGroupNamePrefix=log_group_name
        )
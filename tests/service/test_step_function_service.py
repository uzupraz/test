import json

import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from exception import ServiceException
from service import StepFunctionService
from utils import Singleton


class TestStepFunctionService(unittest.TestCase):


    @patch("service.step_function_service.boto3.client")
    def setUp(self, mock_boto3_client) -> None:
        mock_boto3_client.return_value = MagicMock()
        self.aws_config = MagicMock()
        self.aws_config.sqs_workflow_billing_arn = "arn:aws:sqs:region:account-id:queue-name"
        Singleton.clear_instance(StepFunctionService)
        self.step_function_service = StepFunctionService(self.aws_config)


    def tearDown(self) -> None:
        self.aws_config = None
        self.step_function_service = None
        Singleton.clear_instance(StepFunctionService)

    
    def test_create_state_machine_success(self):
        """Test that a state machine is created successfully with the correct ARN returned."""
        payload = MagicMock()
        payload.state_machine_name = "TestStateMachine"
        payload.state_machine_definition = {"StartAt": "Step1", "States": {}}
        payload.execution_role_arn = "arn:aws:iam::account-id:role/ExecutionRole"
        payload.type = "EXPRESS"
        payload.logging_configuration = {}

        self.step_function_service.stepfunctions.create_state_machine = MagicMock(return_value = {
            "stateMachineArn": "arn:aws:states:region:account-id:stateMachine:TestStateMachine"
        })

        result = self.step_function_service.create_state_machine(payload)

        self.assertEqual(
            result,
            "arn:aws:states:region:account-id:stateMachine:TestStateMachine"
        )
        self.step_function_service.stepfunctions.create_state_machine.assert_called_once_with(
            name=payload.state_machine_name,
            definition=json.dumps(payload.state_machine_definition),
            roleArn=payload.execution_role_arn,
            type=payload.type,
            loggingConfiguration=payload.logging_configuration,
        )


    def test_create_state_machine_failure(self):
        """Test that a ServiceException is raised when state machine creation fails."""
        payload = MagicMock()
        payload.state_machine_name = "TestStateMachine"
        payload.state_machine_definition = {"StartAt": "Step1", "States": {}}
        payload.execution_role_arn = "arn:aws:iam::account-id:role/ExecutionRole"
        payload.type = "EXPRESS"
        payload.logging_configuration = {}

        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.step_function_service.stepfunctions.create_state_machine = MagicMock(side_effect = ClientError(error_response, "create_state_machine"))

        with self.assertRaises(ServiceException) as context:
            self.step_function_service.create_state_machine(payload)

        self.assertEqual(context.exception.message, "Failed to create state machine")


    def test_update_state_machine_success(self):
        """Test that an existing state machine is updated successfully."""
        payload = MagicMock()
        payload.state_machine_arn = "arn:aws:states:region:account-id:stateMachine:TestStateMachine"
        payload.state_machine_definition = {"StartAt": "Step1", "States": {}}
        payload.execution_role_arn = "arn:aws:iam::account-id:role/ExecutionRole"

        self.step_function_service.stepfunctions.update_state_machine = MagicMock()

        self.step_function_service.update_state_machine(payload)

        self.step_function_service.stepfunctions.update_state_machine.assert_called_once_with(
            stateMachineArn=payload.state_machine_arn,
            definition=json.dumps(payload.state_machine_definition),
            roleArn=payload.execution_role_arn
        )


    def test_update_state_machine_failure(self):
        """Test that a ServiceException is raised when updating a state machine fails."""
        payload = MagicMock()
        payload.state_machine_arn = "arn:aws:states:region:account-id:stateMachine:TestStateMachine"
        payload.state_machine_definition = {"StartAt": "Step1", "States": {}}
        payload.execution_role_arn = "arn:aws:iam::account-id:role/ExecutionRole"

        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.step_function_service.stepfunctions.update_state_machine = MagicMock(side_effect = ClientError(error_response, "update_state_machine"))

        with self.assertRaises(ServiceException) as context:
            self.step_function_service.update_state_machine(payload)

        self.assertEqual(context.exception.message, "Failed to update state machine")


    def test_get_lambda_task_definition(self):
        """Test that a valid Lambda task definition is generated with correct parameters."""
        task_def = self.step_function_service.get_lambda_task_definition(
            resource="arn:aws:lambda:region:account-id:function:TestFunction",
            state_machine_arn="arn:aws:states:region:account-id:stateMachine:TestStateMachine",
            payload={"key": "value"},
            next_state="NextState"
        )

        self.assertEqual(task_def["Type"], "Task")
        self.assertEqual(task_def["Resource"], "arn:aws:lambda:region:account-id:function:TestFunction")
        self.assertEqual(task_def["Next"], "NextState")
        self.assertListEqual(task_def["Retry"], self.step_function_service.get_default_lambda_retry_policy())


    def test_get_lambda_task_definition_with_custom_retry_policy(self):
        """Test that a valid Lambda task definition is generated with custom retry policy."""
        policy = [{
            "ErrorEquals": [
                "Lambda.ServiceException",
            ],
            "IntervalSeconds": 2,
            "MaxAttempts": 1,
            "BackoffRate": 1
        }]
        task_def = self.step_function_service.get_lambda_task_definition(
            resource="arn:aws:lambda:region:account-id:function:TestFunction",
            state_machine_arn="arn:aws:states:region:account-id:stateMachine:TestStateMachine",
            payload={"key": "value"},
            next_state="NextState",
            retry_policy=policy
        )

        self.assertEqual(task_def["Type"], "Task")
        self.assertEqual(task_def["Resource"], "arn:aws:lambda:region:account-id:function:TestFunction")
        self.assertEqual(task_def["Next"], "NextState")
        self.assertListEqual(task_def["Retry"], policy)


    def test_get_workflow_billing_definition(self):
        """Test that the workflow billing task definition is constructed correctly."""
        billing_def = self.step_function_service.get_workflow_billing_definition()

        self.assertEqual(billing_def["Type"], "Task")
        self.assertEqual(billing_def["Resource"], "arn:aws:states:::sqs:sendMessage")
        self.assertEqual(billing_def["Parameters"]["QueueUrl"], self.aws_config.sqs_workflow_billing_arn)
        self.assertTrue(billing_def["End"])


    def test_get_default_lambda_retry_policy(self):
        """Test that the default Lambda retry policy is generated correctly."""
        retry_policy = self.step_function_service.get_default_lambda_retry_policy()

        self.assertIsInstance(retry_policy, list)
        self.assertEqual(retry_policy[0]["ErrorEquals"], [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
        ])
        self.assertEqual(retry_policy[0]["IntervalSeconds"], 1)
        self.assertEqual(retry_policy[0]["MaxAttempts"], 3)
        self.assertEqual(retry_policy[0]["BackoffRate"], 2)


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

        result = self.step_function_service.get_logging_configuration(log_group_arn)

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

        result = self.step_function_service.get_logging_configuration(log_group_arn, level="ERROR")

        self.assertEqual(result, expected_config)
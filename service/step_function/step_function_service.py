import boto3
import json

from botocore.exceptions import ClientError

from model import StateMachineCreatePayload, StateMachineUpdatePayload
from utils import Singleton
from  configuration import AWSConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class StepFunctionService(metaclass=Singleton):


    def __init__(self, aws_config: AWSConfig) -> None:
        """
        Initializes the service with the provided AWS configuration.

        Args:
            aws_config (AWSConfig): The configuration object containing AWS-related details such as ARNs and execution roles.
        """
        self.aws_config = aws_config
        self.stepfunctions = boto3.client("stepfunctions")


    def create_state_machine(self, payload: StateMachineCreatePayload) -> str:
        """
        Creates a new state machine in AWS Step Functions.

        Args:
            payload (StateMachineCreatePayload): Contains the state machine configuration.

        Returns:
            str: The ARN of the created state machine.

        Raises:
            ServiceException: If state machine creation fails.
        """
        log.info("Creating state machine. name: %s", payload.state_machine_name)
        try:
            response = self.stepfunctions.create_state_machine(
                name=payload.state_machine_name,
                definition=json.dumps(payload.state_machine_defination),
                roleArn=payload.execution_role_arn,
                type=payload.type,
                loggingConfiguration=payload.logging_configuration,
            )
            log.info("State machine updated successfully. name: %s", payload.state_machine_name)
            return response["stateMachineArn"]
        except ClientError as e:
            log.exception("Failed to create state machine. name: %s", payload.state_machine_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to create state machine')


    def update_state_machine(self, payload: StateMachineUpdatePayload) -> None:
        """
        Updates an existing state machine's definition or role.

        Args:
            payload (StateMachineUpdatePayload): Contains the ARN, new definition, and role ARN.

        Raises:
            ServiceException: If updating the state machine fails.
        """
        log.info("Updating state machine. arn: %s", payload.state_machine_arn)
        try:
            self.stepfunctions.update_state_machine(
                stateMachineArn=payload.state_machine_arn,
                definition=json.dumps(payload.state_machine_defination),
                roleArn=payload.execution_role_arn
            )
            log.info("State machine updated successfully. arn: %s", payload.state_machine_arn)
        except ClientError as e:
            log.exception("Failed to update state machine. arn: %s", payload.state_machine_arn)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to update state machine')


    def get_task_definition(self, resource: str, state_machine_arn: str, payload: dict, next_state: str, output_path: str = "$.Payload", retry_policy: list[dict] = None):
        """
        Builds a task definition for a state in AWS Step Functions.

        Args:
            resource (str): The ARN of the Lambda function or activity resource.
            state_machine_arn (str): ARN of the target state machine.
            payload (dict): Data payload to pass to the task.
            retry_policy (list[dict]): List of retry configurations.
            next_state (str): The next state to execute after the task completes.
            output_path (str): The output path after the task completes.

        Returns:
            dict: Configuration dictionary for a Step Functions Task state.
        """
        return {
            "Type": "Task",
            "Resource": resource,
            "OutputPath": output_path,
            "Parameters": {
                "FunctionName": state_machine_arn,
                "Payload": payload
            },
            "Retry": retry_policy if retry_policy else self.__get_default_retry_policy(),
            "Next": next_state
        }


    def get_workflow_billing_definition(self):
        """
        Constructs the billing task definition for the state machine.

        Returns:
            dict: The task definition for the billing state.
        """
        return {
            "Type": "Task",
            "Resource": "arn:aws:states:::sqs:sendMessage",
            "Parameters": {
                "QueueUrl": self.aws_config.sqs_workflow_billing_arn,
                "MessageBody": {
                    "executionArn.$": "$$.Execution.Id",
                    "workflowId.$": "$.attributes.workflowId",
                    "ownerId.$": "$.attributes.ownerId",
                    "eventId.$": "$.attributes.eventId",
                    "executionId.$": "$.attributes.executionId",
                    "executionStartTime.$": "$$.Execution.StartTime"
                }
            },
            "Retry": [
                {
                    "ErrorEquals": [
                        "States.ALL"
                    ],
                    "IntervalSeconds": 1,
                    "MaxAttempts": 3,
                    "BackoffRate": 2
                }
            ],
            "Catch": [
                {
                "ErrorEquals": [
                    "States.ALL"
                ],
                "Next": "EndState"
                }
            ],
            "End": True,
            "ResultPath": None
        }
    

    def __get_default_retry_policy(self):
        return [
            {
                "ErrorEquals": [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException"
                ],
                "IntervalSeconds": 1,
                "MaxAttempts": 3,
                "BackoffRate": 2
            }
        ]
    
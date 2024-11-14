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


    def get_task_definition(self, resource: str, state_machine_arn: str, payload: dict, retry_policy: list[dict], next_state: str):
        return {
            "Type": "Task",
            "Resource": resource,
            "OutputPath": "$.Payload",
            "Parameters": {
                "FunctionName": state_machine_arn,
                "Payload": payload
            },
            "Retry": retry_policy,
            "Next": next_state
        }
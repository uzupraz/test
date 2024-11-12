import boto3

from botocore.exceptions import ClientError
from typing import List

from configuration import AWSConfig
from service import WorkflowService
from controller import common_controller as common_ctrl
from utils import Singleton
from model import DataStudioMapping
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class DataStudioMappingStepFunctionService(metaclass=Singleton):

    MAPPER = 'Mapper'
    BILLING = 'Billing'

    def __init__(self, aws_config: AWSConfig, workflow_service: WorkflowService) -> None:
        self.aws_config = aws_config
        self.stepfunctions = boto3.client("stepfunctions")
        self.workflow_service = workflow_service
        self.lambda_arns = {
            self.MAPPER: "arn:aws:lambda:region:account-id:function:Mapper",
            "csv_parser": "arn:aws:lambda:region:account-id:function:CsvParser",
            "csv_writer": "arn:aws:lambda:region:account-id:function:CsvWriter",
            "json_parser": "arn:aws:lambda:region:account-id:function:JsonParser",
            "json_writer": "arn:aws:lambda:region:account-id:function:JsonWriter"
        }


    def create_mapping_state_machine(self, mapping: DataStudioMapping) -> str:
        try:
            definition = self._create_definition_from_mapping(mapping)
            response = self.stepfunctions.create_state_machine(
                name=mapping.name,
                definition=str(definition),
                roleArn=self.aws_config.stepfunctions_execution_role_arn
            )
            return response["stateMachineArn"]
        except ClientError as e:
            log.exception('Failed to create step function. owner_id: %s, mapping_id: %s', mapping.owner_id, mapping.id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to create step function')


    def update_mapping_state_machine(self, mapping: DataStudioMapping, state_machine_arn: str) -> None:
        try:
            definition = self._create_definition_from_mapping(mapping)
            self.stepfunctions.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=str(definition),
                roleArn=self.aws_config.stepfunctions_execution_role_arn
            )
        except ClientError as e:
            log.exception('Failed to update step function. owner_id: %s, mapping_id: %s, arn: %s', mapping.owner_id, mapping.id, state_machine_arn)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to update step function')


    def _create_definition_from_mapping(self, mapping: DataStudioMapping) -> dict:
        # Formats
        input_format = mapping.sources.get("input", {}).get("format", None)
        output_format = mapping.output.get("format", None)

        if not input_format or not output_format:
            log.error("Unable to find input or output format. mapping_id: %s", mapping.id)
            raise ValueError("Unable to find input or output format. mapping_id: %s", mapping.id)
        
        # Parameters
        parser_parameters = mapping.sources.get("input", {}).get("parameters", {})
        writer_parameters = mapping.output.get("parameters", {})
        parser_parameters['wrapJson'] = "true"
        writer_parameters['unwrapJson'] = 'true'
        
        # Arn
        mapper_arn = self.lambda_arns.get(self.MAPPER, None)
        parser_arn = self.lambda_arns.get(input_format + "_parser", None)
        writer_arn = self.lambda_arns.get(output_format + "_writer", None)

        if not mapper_arn or not parser_arn or not writer_arn:
            log.error("Unable to find arn for mapper, parser or writer. mapping_id: %s", mapping.id)
            raise ValueError("Unable to find arn for mapper, parser or writer for mapping: %s", mapping.id)
        
        # Definition
        parser_definition = self._get_task_definition(parser_arn, parser_parameters, self.MAPPER)
        mapper_definition = self._get_task_definition(mapper_arn, {}, output_format + " writer")
        writer_definition = self._get_task_definition(writer_arn, writer_parameters, self.BILLING)
        
        return {
            "Comment": mapping.description,
            "StartAt": input_format + " parser",
            "States": {
                input_format + " parser": parser_definition,
                self.MAPPER: mapper_definition,
                output_format + " writer": writer_definition,
                "Billing": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": self.aws_config.sqs_billing_arn,
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
                },
                "EndState": {
                    "Type": "Succeed"
                }
            }
        }


    def _get_task_definition(self, state_machine_arn: str, parameters: dict, next_state: str):
        return {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "OutputPath": "$.Payload",
            "Parameters": {
                "FunctionName": state_machine_arn,
                "Payload": {
                    "body.$": "$.body",
                    "attributes.$": "$.attributes",
                    "parameters": parameters
                }
            },
            "Retry": self._get_retry_policy(),
            "Next": next_state
        }
    

    def _get_retry_policy(self) -> List[dict]:
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
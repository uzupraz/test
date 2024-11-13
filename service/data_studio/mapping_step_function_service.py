import boto3
import json

from botocore.exceptions import ClientError
from typing import List, Tuple

from configuration import AWSConfig
from repository import DataFormatsRepository
from controller import common_controller as common_ctrl
from utils import Singleton
from model import DataStudioMapping, DataFormat
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class DataStudioMappingStepFunctionService(metaclass=Singleton):

    MAPPER = 'Mapper'
    BILLING = 'Billing'
    CLOUD_WATCH_RETENSION_IN_DAYS= 180

    def __init__(self, aws_config: AWSConfig, data_formats_repository: DataFormatsRepository) -> None:
        """
        Initializes the service with the provided AWS configuration.

        Args:
            aws_config (AWSConfig): The configuration object containing AWS-related details such as ARNs and execution roles.
            data_formats_repository (DataFormatsRepository): The DataFormatsRepository provides method related to data formats used by data studio.
        """
        self.aws_config = aws_config
        self.data_formats_repository = data_formats_repository
        
        self.stepfunctions = boto3.client("stepfunctions")
        self.logs_client = boto3.client('logs')


    def create_mapping_state_machine(self, mapping: DataStudioMapping, type: str = 'EXPRESS') -> str:
        """
        Creates a new Step Functions state machine for the provided mapping.

        Args:
            mapping (DataStudioMapping): The mapping object containing the configuration for the state machine.
            type (str, optional): The type of the state machine 'EXPRESS' or 'STANDARD'. Defaults to 'EXPRESS'.

        Returns:
            str: The ARN of the created state machine.

        Raises:
            ServiceException: If there's an error while creating the state machine.
        """
        log.info("Creating state machine for the provided mapping. owenr_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
        try:
            definition = self._create_definition_from_mapping(mapping)
            name = f"{mapping.owner_id}-{mapping.id}"

            # Logging
            log.info("Creating log groups. owenr_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
            log_group_arn = self.aws_config.cloudwatch_log_group_base_arn + f"/{name}-Logs:*"
            log_group_name = log_group_arn.split('log-group:')[1].split(':')[0]
            self._create_log_group_if_not_exist(log_group_name)

            response = self.stepfunctions.create_state_machine(
                name=name,
                definition=json.dumps(definition),
                roleArn=self.aws_config.stepfunctions_execution_role_arn,
                type=type,
                loggingConfiguration=self._get_logging_configurations(log_group_arn),
                tracingConfiguration=self._get_tracking_configuration(),
            )
            log.info("State machine created successfully. owenr_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
            return response["stateMachineArn"]
        except ClientError as e:
            log.exception('Failed to create step function. owner_id: %s, mapping_id: %s', mapping.owner_id, mapping.id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to create step function')


    def update_mapping_state_machine(self, mapping: DataStudioMapping, state_machine_arn: str) -> None:
        """
        Updates an existing Step Functions state machine with the new mapping definition.

        Args:
            mapping (DataStudioMapping): The mapping object containing the new configuration.
            state_machine_arn (str): The ARN of the state machine to be updated.

        Raises:
            ServiceException: If there's an error while updating the state machine.
        """
        log.info("Updating state machine for the provided mapping & arn. owenr_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
        try:
            definition = self._create_definition_from_mapping(mapping)
            self.stepfunctions.update_state_machine(
                stateMachineArn=state_machine_arn,
                definition=json.dumps(definition),
                roleArn=self.aws_config.stepfunctions_execution_role_arn
            )
            log.info("State machine updated successfully. owenr_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
        except ClientError as e:
            log.exception('Failed to update step function. owner_id: %s, mapping_id: %s, arn: %s', mapping.owner_id, mapping.id, state_machine_arn)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to update step function')


    def _create_log_group_if_not_exist(self, log_group_name: str):
        """
        Checks if the specified CloudWatch Log Group exists and creates it if it doesn't.

        Args:
            log_group_name (str): The name of the CloudWatch Log Group to check/create.

        Returns:
            bool: True if the log group exists or was successfully created.

        Raises:
            ServiceException: If there's an error while checking or creating the log group.
        """
        try:
            response = self.logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
            if any(group['logGroupName'] == log_group_name for group in response['logGroups']):
                log.info("Log group already exists. log_group_name: %s", log_group_name)
                return True
            
            self.logs_client.create_log_group(logGroupName=log_group_name)
            log.info("Log group created. log_group_name: %s", log_group_name)

            self.logs_client.put_retention_policy(logGroupName=log_group_name, retentionInDays=self.CLOUD_WATCH_RETENSION_IN_DAYS)
            log.info("Retention policy set to 180 days for log_group_name: %s", log_group_name)

            return True
        except ClientError as e:
            log.exception('Failed to create log group. log_group_name: %s', log_group_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to create log group')


    def _create_definition_from_mapping(self, mapping: DataStudioMapping) -> dict:
        """
        Creates the state machine definition based on the provided mapping.

        Args:
            mapping (DataStudioMapping): The mapping object containing the configuration.

        Returns:
            dict: The Step Functions state machine definition.
        
        Raises:
            ValueError: If input or output formats are missing or invalid.
        """
        (parser_name, parser_arn), (writer_name, writer_arn) = self._get_parser_and_writer_details(mapping)
        
        # Parameters
        parser_parameters = mapping.sources.get("input", {}).get("parameters", {})
        writer_parameters = mapping.output.get("parameters", {})

        if not mapping.mapping:
            log.error("Mapping schema not found. owner_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
            raise ServiceException(400, ServiceStatus.FAILURE, "Unable to find mapping schema.")
        mapper_parameters = {"mappingSchema": mapping.mapping}

        # Wrap & Unwap Json
        parser_parameters['wrapJson'] = "true"
        writer_parameters['unwrapJson'] = 'true'
        
        # Definition
        parser_definition = self._get_task_definition(parser_arn, parser_parameters, self.MAPPER)
        mapper_definition = self._get_task_definition(self.aws_config.map_processor_arn, mapper_parameters, writer_name)
        writer_definition = self._get_task_definition(writer_arn, writer_parameters, self.BILLING)
        billing_defination = self._get_billing_definition()

        return {
            "Comment": mapping.description,
            "StartAt": parser_name,
            "States": {
                parser_name: parser_definition,
                self.MAPPER: mapper_definition,
                writer_name: writer_definition,
                self.BILLING: billing_defination,
                "EndState": {
                    "Type": "Succeed"
                }
            }
        }


    def _get_parser_and_writer_details(self, mapping: DataStudioMapping) -> Tuple[Tuple[str, str], Tuple[str, str]]:
        """
        Getting parser & writer based on the provided mapping.

        Args:
            mapping (DataStudioMapping): The mapping object containing the configuration.

        Returns:
            tuple(tuple(parser_name, parser_arn), tuple(writer_name, writer_arn)): The parser & writer details of required for mapping.
        
        Raises:
            ValueError: If input or output formats are missing or invalid.
        """
        # Formats
        input_format_name = mapping.sources.get("input", {}).get("format", None).upper()
        output_format_name = mapping.output.get("format", None).upper()
        
        input_format = self.data_formats_repository.get_data_format(input_format_name)
        output_format = input_format
        # Preventing over fetching for same data format name
        if input_format_name != output_format_name:
            output_format = self.data_formats_repository.get_data_format(output_format_name)

        if not input_format or not output_format:
            log.error("Unable to find input or output format. mapping_id: %s", mapping.id)
            raise ServiceException(400, ServiceStatus.FAILURE, "Unable to find input or output format.")
        
        # Names
        parser_name = input_format.name + " Parser"
        writer_name = output_format.name + " Writer"

        # Arn
        parser_arn = input_format.parser.get("lambda_arn")
        writer_arn = output_format.parser.get("lambda_arn")

        return ((parser_name, parser_arn), (writer_name, writer_arn))


    def _get_task_definition(self, state_machine_arn: str, parameters: dict, next_state: str):
        """
        Constructs the task definition for a given Lambda function in the state machine.

        Args:
            state_machine_arn (str): The ARN of the Lambda function to invoke.
            parameters (dict): The parameters to pass to the Lambda function.
            next_state (str): The next state to transition to after the task is completed.

        Returns:
            dict: The task definition for the Lambda function.
        """
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
    

    def _get_logging_configurations(self, log_group_arn: str, level: str = "ALL", includeExecutionData: bool = True):
        """
        Constructs the logging configuration for the state machine.

        Args:
            log_group_arn (str): The ARN of the CloudWatch Log Group.
            level (str, optional): The logging level. Defaults to "ALL".
            includeExecutionData (bool, optional): Whether to include execution data in the logs. Defaults to True.

        Returns:
            dict: The logging configuration for the state machine.
        """
        return {
            "level": level,
            "includeExecutionData": includeExecutionData,
            "destinations": [
                {
                    "cloudWatchLogsLogGroup": {
                        "logGroupArn": log_group_arn
                    }
                }
            ]
        }
    

    def _get_tracking_configuration(self):
        """
        Constructs the tracing configuration for the state machine, enabling AWS X-Ray tracing.

        Returns:
            dict: The tracing configuration.
        """
        return {
            "enabled": False  # Enables AWS X-Ray tracing for this state machine
        }


    def _get_retry_policy(self) -> List[dict]:
        """
        Returns the retry policy for tasks in the state machine.

        Returns:
            List[dict]: The retry policy configuration.
        """
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
    

    def _get_billing_definition(self):
        """
        Constructs the billing task definition for the state machine.

        Returns:
            dict: The task definition for the billing state.
        """
        return {
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
        }
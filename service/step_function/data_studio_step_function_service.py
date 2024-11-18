from .step_function_service import StepFunctionService
from service import AWSCloudWatchService, DataFormatsService
from model import StateMachineCreatePayload, StateMachineUpdatePayload, Workflow, DataStudioMapping
from  configuration import AWSConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus, StateMachineType

from typing import Tuple

log = common_ctrl.log


class DataStudioStepFunctionService(StepFunctionService):

    JSON_TRANSFORMER_PROCESSOR_NAME = "JSON Transformer"
    WORKFLOW_BILLING_PROCESSOR_NAME = "Billing"
    LAMBDA_TASK_RESOURCE = "arn:aws:states:::lambda:invoke"


    def __init__(self, aws_config: AWSConfig, aws_cloudwatch_service: AWSCloudWatchService, data_formats_service: DataFormatsService) -> None:
        """
        Initializes the service with the provided AWS configuration.

        Args:
            aws_config (AWSConfig): The configuration object containing AWS-related details such as ARNs and execution roles.
            aws_cloudwatch_service (AWSCloudWatchService): The cloud watch service object.
            data_formats_service (DataFormatsService): The data format service used to access different data formats available.
        """
        super().__init__(aws_config)
        self.aws_config = aws_config
        self.data_formats_service = data_formats_service
        self.aws_cloudwatch_service = aws_cloudwatch_service


    def create_workflow_state_machine(self, mapping: DataStudioMapping) -> str:
        """
        Creates a new Data Studio workflow with logging, state machine configuration, 
        and saves it in the workflow repository.

        Args:
            mapping (DataStudioMapping): Mapping details used to configure the workflow.
        """
        definition = self.get_data_studio_workflow_state_machine_definition(mapping)
        name = f"{mapping.owner_id}-{mapping.id}"

        # Logging
        log_group_name = self.aws_config.cloudwatch_log_group_base + f"/{name}-Logs"
        log_group_arn = self.aws_cloudwatch_service.create_log_group(log_group_name)
        self.aws_cloudwatch_service.update_retention_policy(log_group_name, int(self.aws_config.cloudwatch_retention_in_days))
        
        payload = StateMachineCreatePayload(
            state_machine_name=name,
            state_machine_definition=definition,
            execution_role_arn=self.aws_config.stepfunction_execution_role_arn,
            type=StateMachineType.EXPRESS.value,
            logging_configuration=self.aws_cloudwatch_service.get_logging_configuration(log_group_arn)
        )
        return self.create_state_machine(payload)
        

    def update_workflow_state_machine(self, mapping: DataStudioMapping, workflow: Workflow):
        """
        Updates an existing Data Studio workflow with a new state machine definition.

        Args:
            mapping (DataStudioMapping): Mapping details with updated workflow configuration.
            workflow (Workflow): Existing workflow to update.
        """
        definition = self.get_data_studio_workflow_state_machine_definition(mapping)
        payload = StateMachineUpdatePayload(
            state_machine_arn=workflow.state_machine_arn, 
            state_machine_definition=definition,
            execution_role_arn=self.aws_config.stepfunction_execution_role_arn
        )
        self.update_state_machine(payload)


    def get_data_studio_workflow_state_machine_definition(self, mapping: DataStudioMapping):
        """
        Generates the state machine definition for the Data Studio workflow.

        Args:
            mapping (DataStudioMapping): Contains workflow mapping details such as 
            parser, writer, and transformer configurations.

        Returns:
            dict: JSON structure defining the states and transitions in the state machine.
        """
        # Validation
        if not mapping.mapping:
            log.error("Mapping schema not found. owner_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
            raise ServiceException(400, ServiceStatus.FAILURE, "Unable to find mapping schema.")
        
        (parser_name, parser_arn), (writer_name, writer_arn) = self.__get_parser_and_writer_details(mapping)

        # Parameters
        parser_parameters = mapping.sources.get("input", {}).get("parameters", {})
        writer_parameters = mapping.output.get("parameters", {})
        json_transformer_parameters = {"mappingSchema": mapping.mapping}

        # Parameters Override
        parser_parameters["wrapOutputIn"] = "input"
        json_transformer_parameters["wrapOutputIn"] = "output"
        writer_parameters["unwrapInputFrom"] = "output"
        
        return {
            "Comment": mapping.description,
            "StartAt": parser_name,
            "States": {
                parser_name: self.get_lambda_task_definition(
                    resource=self.LAMBDA_TASK_RESOURCE,
                    state_machine_arn=parser_arn,
                    payload=self.__get_processor_payload(parser_parameters),
                    next_state=self.JSON_TRANSFORMER_PROCESSOR_NAME
                ),
                self.JSON_TRANSFORMER_PROCESSOR_NAME: self.get_lambda_task_definition(
                    resource=self.LAMBDA_TASK_RESOURCE,
                    state_machine_arn=self.aws_config.json_transformer_processor_arn,
                    payload=self.__get_processor_payload(json_transformer_parameters),
                    next_state=writer_name
                ),
                writer_name: self.get_lambda_task_definition(
                    resource=self.LAMBDA_TASK_RESOURCE,
                    state_machine_arn=writer_arn,
                    payload=self.__get_processor_payload(writer_parameters),
                    next_state=self.WORKFLOW_BILLING_PROCESSOR_NAME
                ),
                self.WORKFLOW_BILLING_PROCESSOR_NAME: self.get_workflow_billing_definition(),
                "EndState": {
                    "Type": "Succeed"
                }
            }
        }


    def __get_processor_payload(self, parameters: dict):
        """
        Constructs the payload structure for state machine tasks.

        Args:
            parameters (dict): Parameters specific to the task being processed.

        Returns:
            dict: Payload formatted with required attributes and parameters.
        """
        return {
            "body.$": "$.body",
            "attributes.$": "$.attributes",
            "parameters": parameters
        }
    

    def __get_parser_and_writer_details(self, mapping: DataStudioMapping) -> Tuple[Tuple[str, str], Tuple[str, str]]:
        """
        Retrieves parser and writer names and ARNs based on the input and output format
        in the provided mapping.

        Args:
            mapping (DataStudioMapping): Mapping details with source and output formats.

        Returns:
            Tuple[Tuple[str, str], Tuple[str, str]]: Pairs of (name, ARN) for the parser 
            and writer components used in the workflow.
        """
        # Formats
        input_format_name = mapping.sources.get("input", {}).get("format", None)
        output_format_name = mapping.output.get("format", None)

        if not input_format_name or not output_format_name:
            log.error("Invalid input or output format in mapping. mapping_id: %s", mapping.id)
            raise ServiceException(400, ServiceStatus.FAILURE, "Invalid input or output format in mapping.")
        
        input_format_db_item = self.data_formats_service.get_data_format(input_format_name.upper())
        output_format_db_item = input_format_db_item
        # Preventing over fetching for same data format name
        if input_format_name != output_format_name:
            output_format_db_item = self.data_formats_service.get_data_format(output_format_name.upper())

        if not input_format_db_item or not output_format_db_item:
            log.error("Unable to find input or output format. mapping_id: %s", mapping.id)
            raise ServiceException(400, ServiceStatus.FAILURE, "Unable to find input or output format.")
        
        # Names
        parser_name = input_format_db_item.format_name + " Parser"
        writer_name = output_format_db_item.format_name + " Writer"

        # Arn
        parser_arn = input_format_db_item.parser.lambda_arn
        writer_arn = output_format_db_item.writer.lambda_arn

        return ((parser_name, parser_arn), (writer_name, writer_arn))
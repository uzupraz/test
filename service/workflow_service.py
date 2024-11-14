from model import Workflow, DataStudioMapping, StateMachineCreatePayload, StateMachineUpdatePayload
from repository import WorkflowRepository
from controller import common_controller as common_ctrl
from utils import Singleton
from .aws_cloudwatch_service import AWSCloudWatchService 
from .step_function_service import StepFunctionService
from .data_formats_service import DataFormatsService
from configuration import AWSConfig
from enums import ServiceStatus
from exception import ServiceException

from typing import Optional, Tuple


log = common_ctrl.log


class WorkflowService(metaclass=Singleton):


    def __init__(self, aws_config: AWSConfig, workflow_repository: WorkflowRepository, data_formats_service: DataFormatsService, aws_cloudwatch_service: AWSCloudWatchService, step_function_service: StepFunctionService) -> None:
        self.aws_config = aws_config
        self.workflow_repository = workflow_repository
        self.data_formats_service = data_formats_service
        self.aws_cloudwatch_service = aws_cloudwatch_service
        self.step_function_service = step_function_service


    def save_workflow(self, workflow: Workflow) -> 'Workflow':
        """
        Saves a workflow using the workflow repository.

        Args:
            workflow (Workflow): The workflow to be saved.

        Returns:
            Workflow: The created workflow object.
        """
        log.info('Calling repository to save workflow. workflowId: %s, organizationId: %s', workflow.workflow_id, workflow.owner_id)
        created_workflow = self.workflow_repository.save(workflow)
        return created_workflow
    

    def get_data_studio_workflows(self, owner_id:str) -> list[Workflow]:
        """
        Returns a list of workflows for the given owner where the mapping_id is present.
        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.
        Returns:
            list[Workflow]: List of workflows for the given owner.
        """
        workflows_response = self.workflow_repository.get_data_studio_workflows(owner_id)
        workflows = [
            Workflow.from_dict(workflow_response)
            for workflow_response in workflows_response
        ]
        return workflows
    

    def get_workflow(self, owner_id:str, workflow_id: str) -> Optional[Workflow]:
        """
        Returns a workflow for the given owner and workflow id.
        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.
            workflow_id (str): The ID for the workflow.
        Returns:
            Optional[Workflow]: Workflow or None for the given owner & workflow id.
        """
        return self.workflow_repository.get_workflow(owner_id, workflow_id)
    

    def create_data_studio_workflow(self, mapping: DataStudioMapping):
        workflow = self.workflow_repository.get_workflow(mapping.owner_id, mapping.id)
        if workflow:
            self._update_data_studio_workflow(mapping, workflow)
        else:
            self._create_data_studio_workflow(mapping)

    
    def _create_data_studio_workflow(self, mapping: DataStudioMapping):
        defination = self._get_data_studio_workflow_state_machine_defination(mapping)
        name = f"{mapping.owner_id}-{mapping.id}"

        # Logging
        log_group_name = self.aws_config.cloudwatch_log_group_base + f"/{name}-Logs"
        log_group_arn = self.aws_cloudwatch_service.create_log_group(log_group_name)
        self.aws_cloudwatch_service.update_retention_policy(log_group_name, int(self.aws_config.cloudwatch_retension_in_days))
        
        payload = StateMachineCreatePayload(
            state_machine_name=name,
            state_machine_defination=defination,
            execution_role_arn=self.aws_config.stepfunction_execution_role_arn,
            type="EXPRESS",
            logging_configuration=self.aws_cloudwatch_service.get_logging_configuration(log_group_arn)
        )
        state_machine_arn = self.step_function_service.create_state_machine(payload)
        workflow = Workflow(
            owner_id=mapping.owner_id,
            workflow_id=mapping.id,
            name=mapping.name,
            event_name=f"es:workflow:{mapping.owner_id}:{mapping.id}",
            created_by=mapping.created_by,
            created_by_name="DataStudio",
            group_name="DataStudio",
            state="ACTIVE",
            version=1,
            is_sync_execution=True,
            state_machine_arn=state_machine_arn,
            is_binary_event=False,
        )
        self.workflow_repository.save(workflow)


    def _update_data_studio_workflow(self, mapping: DataStudioMapping, workflow: Workflow):
        defination = self._get_data_studio_workflow_state_machine_defination(mapping)
        payload = StateMachineUpdatePayload(
            state_machine_arn=workflow.state_machine_arn, 
            state_machine_defination=defination,
            execution_role_arn=self.aws_config.stepfunction_execution_role_arn
        )
        self.step_function_service.update_state_machine(payload)


    def _get_data_studio_workflow_state_machine_defination(self, mapping: DataStudioMapping):
        json_transformer_name = "JSON Transformer"
        workflow_billing_processor_name = "Billing"

        (parser_name, parser_arn), (writer_name, writer_arn) = self._get_parser_and_writer_details(mapping)

        # Parameters
        parser_parameters = mapping.sources.get("input", {}).get("parameters", {})
        writer_parameters = mapping.output.get("parameters", {})
        if not mapping.mapping:
            log.error("Mapping schema not found. owner_id: %s, mapping_id: %s", mapping.owner_id, mapping.id)
            raise ServiceException(400, ServiceStatus.FAILURE, "Unable to find mapping schema.")
        json_transformer_parameters = {"mappingSchema": mapping.mapping}
        
        return {
            "Comment": mapping.description,
            "StartAt": parser_name,
            "States": {
                parser_name: self.step_function_service.get_task_definition(
                    resource="arn:aws:states:::lambda:invoke",
                    state_machine_arn=parser_arn,
                    payload=self._get_processor_payload(parser_parameters),
                    retry_policy=self._get_retry_policy(),
                    next_state=json_transformer_name
                ),
                json_transformer_name: self.step_function_service.get_task_definition(
                    resource="arn:aws:states:::lambda:invoke",
                    state_machine_arn=self.aws_config.json_transformer_processor_arn,
                    payload=self._get_processor_payload(json_transformer_parameters),
                    retry_policy=self._get_retry_policy(),
                    next_state=writer_name
                ),
                writer_name: self.step_function_service.get_task_definition(
                    resource="arn:aws:states:::lambda:invoke",
                    state_machine_arn=writer_arn,
                    payload=self._get_processor_payload(writer_parameters),
                    retry_policy=self._get_retry_policy(),
                    next_state=workflow_billing_processor_name
                ),
                workflow_billing_processor_name: self._get_billing_definition(),
                "EndState": {
                    "Type": "Succeed"
                }
            }
        }


    def _get_processor_payload(self, parameters: dict):
        return {
            "body.$": "$.body",
            "attributes.$": "$.attributes",
            "parameters": parameters
        }
    

    def _get_retry_policy(self):
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
    

    def _get_parser_and_writer_details(self, mapping: DataStudioMapping) -> Tuple[Tuple[str, str], Tuple[str, str]]:
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
        parser_name = input_format_db_item.name + " Parser"
        writer_name = output_format_db_item.name + " Writer"

        # Arn
        parser_arn = input_format_db_item.parser.get("lambda_arn")
        writer_arn = output_format_db_item.parser.get("lambda_arn")

        return ((parser_name, parser_arn), (writer_name, writer_arn))
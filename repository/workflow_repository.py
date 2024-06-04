import boto3
import dataclasses
from botocore.config import Config
from botocore.exceptions import ClientError

from model import Workflow
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class WorkflowRepository:


    _instance = None


    def __init__(self, app_config:AppConfig, aws_config:AWSConfig) -> None:
        self.aws_config = aws_config
        self.app_config = app_config
        self.workflow_table = self.__configure_table()


    def save(self, workflow: Workflow) -> 'Workflow':
        """
        Save the given workflow object to DynamoDB.

        Parameters:
            workflow (Workflow): The workflow object to be saved.

        Returns:
            Workflow: The saved workflow object.

        Raises:
            ServiceException: If there is an error while saving the workflow.
        """
        log.info('Saving workflow. workflowId: %s, organizationId:%s', workflow.workflow_id, workflow.owner_id)
        try:
            # Convert the Workflow object to a dictionary
            workflow_dict = dataclasses.asdict(workflow)
            # Save the dictionary to DynamoDB
            response = self.workflow_table.put_item(Item=workflow_dict)
            log.info('Successfully saved workflow. workflowId: %s, organizationId:%s', workflow.workflow_id, workflow.owner_id)
            return workflow
        except ClientError as e:
            log.exception('Failed to save workflow. workflowId: %s, organizationId:%s', workflow.workflow_id, workflow.owner_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Coulnd\'t save the workflow')
        
    def get_workflow_stats(self, start_date:str, end_date:str) -> dict[str, int|str]:
        """
        Get the stats about the workflows.
        
        Parameters:
            start_date: Start date for the stats.
            end_date: End date for the stats.
        
        Returns:
            active_workflows: Number of active workflows.
            failed_events: Number of failed events.
            fluent_executions: Number of fluent executions.
            system_status: System status.
        
        Raises:
            ServiceException: If there is an error while getting the workflow stats.
        """
        try:
            #! REPLACE THIS WITH REAL DB QUERY
            log.info('Getting workflow stats. start_date: %s, end_date: %s', start_date, end_date)
            return {
                'active_workflows': 10,
                'failed_events': 2,
                'fluent_executions': 8,
                'system_status': 'OK'
            }
        except ClientError as e:
            log.exception('Failed to get workflow stats.')
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Couldn\'t get workflow stats')
        
    def get_workflow_integrations(self, start_date:str, end_date:str) -> dict[str, any]:
        """
        Get all the active workflow integrations.

        Parameters:
            start_date (str): Start date for the workflow integrations.
            end_date (str): End date for the workflow integrations.

        Returns:
            dict[str, any]: Active workflow integrations.
        
        Raises:
            ServiceException: If there is an error while getting the workflow integrations.
        """
        try:
            log.info('Getting workflow integrations. start_date: %s, end_date: %s', start_date, end_date)
            #! REPLACE THIS WITH REAL DB QUERY
            return [
                {
                    "failure_count": 2,
                    "failure_ratio": 0.2,
                    "last_event_date": "2021-07-01",
                    "workflow": {
                        "id": "1",
                        "name": "Workflow 1"
                    }
                }
            ]
        except ClientError as e:
            log.exception('Failed to get workflow integrations.')
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Couldn\'t get workflow integrations')


    def __configure_table(self):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.
        """
        dynamo_db = None

        if self.aws_config.is_local:
            dynamo_db = boto3.resource('dynamodb', region_name = self.aws_config.dynamodb_aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.dynamodb_aws_region)
            dynamo_db = boto3.resource('dynamodb', config = config)

        return dynamo_db.Table(self.app_config.workflow_table_name)


    @classmethod
    def get_instance(cls, app_config:AppConfig, aws_config:AWSConfig, prefer=None) -> 'WorkflowRepository':
        """
        Creates and returns an instance of the WorkflowRepository class.

        Parameters:
            app_config (AppConfig): The AppConfig object used to configure the WorkflowRepository.
            aws_config (AWSConfig): The AWSConfig object used to configure the WorkflowRepository.
            prefer (Optional): An optional WorkflowRepository object that will be used as the instance if provided.

        Returns:
            WorkflowRepository: The instance of the WorkflowRepository class.
        """
        if not cls._instance:
            cls._instance = prefer if prefer else WorkflowRepository(app_config, aws_config)

        return cls._instance
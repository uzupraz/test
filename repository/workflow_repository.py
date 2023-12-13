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


    def __configure_table(self):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.
        """
        dynamo_db = None

        if self.aws_config.is_local:
            dynamo_db = boto3.resource('dynamodb', region_name = self.aws_config.aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.aws_region)
            dynamo_db = boto3.resource('dynamodb', config = config)

        return dynamo_db.Table(self.app_config.workflow_table_name)


    @classmethod
    def get_instance(cls, app_config:AppConfig, aws_config:AWSConfig, prefer=None):
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
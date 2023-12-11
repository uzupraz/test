import boto3
import dataclasses
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from model import Workflow
from configuration import AWSConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class WorkflowRepository:


    _instance = None


    def __init__(self, config:AWSConfig) -> None:
        self.config = config
        self.workflow_table = self.__configure_table()


    def save(self, workflow: Workflow) -> 'Workflow':
        log.info('Saving workflow. workflowId: %s', workflow.workflowId)
        try:
            # Convert the Workflow object to a dictionary
            workflow_dict = dataclasses.asdict(workflow)
            # Save the dictionary to DynamoDB
            response = self.workflow_table.put_item(Item=workflow_dict)
            log.info('Successfully saved workflow. workflowId: %s', workflow.workflowId)
            return workflow
        except ClientError as e:
            log.exception('Failed to save workflow. workflowId: %s', workflow.workflowId, e)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, e.response['Error']['Message'])


    def __configure_table(self):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.
        """
        dynamo_db = None

        if self.config.is_local:
            dynamo_db = boto3.resource('dynamodb', region_name = self.config.aws_region, endpoint_url = 'http://localhost:8000')
        else:
            aws_config = Config(region_name = self.config.aws_region)
            dynamo_db = boto3.resource('dynamodb', config = aws_config)

        return dynamo_db.Table(self.config.workflow_table_name)


    @classmethod
    def get_instance(cls, config:AWSConfig, prefer=None):
        """
        Creates and returns an instance of the WorkflowRepository class.

        Parameters:
            config (AWSConfig): The AWSConfig object used to configure the WorkflowRepository.
            prefer (Optional): An optional WorkflowRepository object that will be used as the instance if provided.

        Returns:
            WorkflowRepository: The instance of the WorkflowRepository class.
        """
        if not cls._instance:
            cls._instance = prefer if prefer else WorkflowRepository(config)

        return cls._instance
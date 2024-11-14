import boto3
import dataclasses
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from typing import Optional

from model import Workflow
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

log = common_ctrl.log


class WorkflowRepository(metaclass=Singleton):


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
            # Save the dictionary to DynamoDB
            self.workflow_table.put_item(Item=workflow.as_dict())
            log.info('Successfully saved workflow. workflow_id: %s, owner_id: %s', workflow.workflow_id, workflow.owner_id)
            return workflow
        except ClientError as e:
            log.exception('Failed to save workflow. workflow_id: %s, owner_id: %s', workflow.workflow_id, workflow.owner_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Coulnd\'t save the workflow')


    def get_data_studio_workflows(self, owner_id:str) -> list[dict]:
        """Returns a list of workflows for the given owner where the mapping_id is present.

        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.

        Returns:
            list[dict]: List of datastudio workflows for the given owner.
        """
        log.info('Getting data studio workflows. owner_id: %s', owner_id)
        try:
            workflows = self.workflow_table.query(
                KeyConditionExpression=Key("ownerId").eq(owner_id) ,
                FilterExpression=Attr("mapping_id").exists() & Attr("mapping_id").ne(None)
            )
            return workflows["Items"]
        except ClientError as e:
            log.exception('Failed to list data studio workflows. owner_id: %s', owner_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Coulnd\'t list data studio workflows')


    def get_workflow(self, owner_id:str, workflow_id:str) -> Optional[Workflow]:
        """Returns a workflow for the given owner & workflow id.

        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.
            workflow_id (str): The workflow ID for which the workflows are to be returned.

        Returns:
            Optional[Workflow]: Datastudio workflow for the given owner & workflow id.
        """
        log.info('Getting workflow. owner_id: %s, workflow_id: %s', owner_id, workflow_id)
        try:
            response = self.workflow_table.query(
                KeyConditionExpression=Key("ownerId").eq(owner_id) & Key("workflowId").eq(workflow_id),
            )
            workflows = response.get("Items", [])

            if not workflows:
                log.error('Unable to find workflow. owner_id: %s, workflow_id: %s', owner_id, workflow_id)
                return None
            
            return Workflow.from_dict(workflows[0])
        except ClientError as e:
            log.exception('Failed to retrieve workflow. owner_id: %s, workflow_id: %s', owner_id, workflow_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Failed to retrieve workflow')


    def count_active_workflows(self, owner_id: str) -> int:
        """
        Count the number of workflows with state 'ACTIVE' for a specific owner.

        Parameters:
            owner_id (str): The ID of the owner whose active workflows are to be counted.

        Returns:
            int: The number of workflows with state 'ACTIVE' for the specified owner.

        Raises:
            ServiceException: If there is an error while counting the workflows.
        """
        log.info('Counting active workflows. owner_id: %s', owner_id)
        try:
            response = self.workflow_table.query(
                KeyConditionExpression=Key('ownerId').eq(owner_id),
                FilterExpression=Attr('state').eq('ACTIVE')
            )
            log.info('Successfully counted active workflows. owner_id: %s', owner_id)
            return response['Count']
        except ClientError as e:
            log.exception('Failed to count active workflows. owner_id: %s', owner_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Couldn\'t count the active workflows')


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

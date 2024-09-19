import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dacite import from_dict
from dataclasses import asdict
from typing import List

from configuration import AppConfig, AWSConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton, DataTypeUtils
from model import CustomScript, CustomScriptUnPublishedChange, CustomScriptRelease

log = common_ctrl.log

class CustomScriptRepository(metaclass=Singleton):


    def __init__(self, app_config:AppConfig, aws_config: AWSConfig) -> None:
        """
        Initialize the CustomScriptRepository with the AWS and App configurations.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.app_config = app_config
        self.aws_config = aws_config

        self.dynamodb_resource = self.__configure_dynamodb_resource()
        self.table = self.dynamodb_resource.Table(self.app_config.custom_script_table_name)


    def get_owner_custom_scripts(self, owner_id: str) -> List[CustomScript]:
        """
        Retrieves all custom scripts associated with a given owner from the DynamoDB table.

        Args:
            owner_id (str): The owner's ID.

        Returns:
            List[CustomScript]: A list of custom scripts belonging to the owner.

        Raises:
            ServiceException: If there is an issue querying the DynamoDB table.
        """
        log.info('Retrieving custom scripts. owner_id: %s', owner_id)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('owner_id').eq(owner_id)
            )
            items = response.get('Items')
            custom_scripts = []
            for script in items:
                item = DataTypeUtils.convert_decimals_to_floats(script)
                custom_scripts.append(from_dict(CustomScript, item))
            
            return custom_scripts
        except ClientError as e:
            log.exception('Failed to retrieve custom script. owner_id: %s, script_id: %s,', owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to create custom script')
        
    
    def get_custom_script(self, owner_id: str, script_id: str) -> CustomScript:
        """
        Retrieves a specific custom script by owner ID and script ID.

        Args:
            owner_id (str): The owner's ID.
            script_id (str): The script ID.

        Returns:
            CustomScript: The custom script object.

        Raises:
            ServiceException: If the script does not exist or the query fails.
        """
        log.info('Retrieving custom script. owner_id: %s, script_id: %s, script_id: %s', owner_id, script_id)
        try:
            response = self.table.get_item(
                Key={'owner_id': owner_id, 'script_id': script_id}
            )
            item = response.get('Item')
            if not item:
                log.error('Customer table item does not exist. owner_id: %s, script_id: %s', owner_id, script_id)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Custom scrpt does not exists')
            
            log.info('Successfully retrieved custom script. owner_id: %s, script_id: %s, script_id: %s', owner_id, script_id)
            item = DataTypeUtils.convert_decimals_to_floats(item)
            return from_dict(CustomScript, item)
        except ClientError as e:
            log.exception('Failed to retrieve custom script. owner_id: %s, script_id: %s, script_id: %s', owner_id, script_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to create custom script')
        

    def create_custom_script(self, item: CustomScript) -> None:
        """
        Creates a new custom script in the DynamoDB table.

        Args:
            item (CustomScript): The custom script object to be saved.

        Raises:
            ServiceException: If there is an issue saving the custom script.
        """
        log.info('Creating custom script. item: %s', item)
        try:
            self.table.put_item(Item=asdict(item))
            log.info('Successfully created custom script. script_id: %s', item.script_id)
        except ClientError as e:
            log.exception('Failed to create custom script. script_id: %s', item.script_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to create custom script')
        
    
    def update_unpublished_changes(self, owner_id: str, script_id: str, unpublished_changes: List[CustomScriptUnPublishedChange]) -> None:
        """
        Updates the unpublished changes for a specific custom script.

        Args:
            owner_id (str): The owner's ID.
            script_id (str): The script ID.
            unpublished_changes (List[CustomScriptUnPublishedChange]): The list of unpublished changes to be updated.

        Raises:
            ServiceException: If the update fails.
        """
        log.info('Updating unpublished changes. script_id: %s, owner_id: %s', script_id, owner_id)
        try:
            unpublished_changes_dict = [asdict(change) for change in unpublished_changes]
            
            response = self.table.update_item(
                Key={
                    'owner_id': owner_id,
                    'script_id': script_id
                },
                UpdateExpression="SET unpublished_changes = :unpublished_changes",
                ExpressionAttributeValues={
                    ':unpublished_changes': unpublished_changes_dict
                },
                ReturnValues="UPDATED_NEW"
            )
            
            log.info('Successfully updated unpublished changes. script_id: %s, owner_id: %s', script_id, owner_id)
            return response
            
        except ClientError as e:
            log.exception('Failed to update unpublished changes. script_id: %s, owner_id: %s', script_id, owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to update unpublished changes')
        

    def update_releases(self, owner_id: str, script_id: str, releases: List[CustomScriptRelease]) -> None:
        """
        Updates the releases for a specific custom script.

        Args:
            owner_id (str): The owner's ID.
            script_id (str): The script ID.
            releases (List[CustomScriptRelease]): The list of releases to be updated.

        Raises:
            ServiceException: If the update fails.
        """
        log.info('Updating releases. script_id: %s, owner_id: %s', script_id, owner_id)
        try:
            releases = [asdict(change) for change in releases]
            
            response = self.table.update_item(
                Key={
                    'owner_id': owner_id,
                    'script_id': script_id
                },
                UpdateExpression="SET releases = :releases",
                ExpressionAttributeValues={
                    ':releases': releases
                },
                ReturnValues="UPDATED_NEW"
            )
            
            log.info('Successfully updated releases. script_id: %s, owner_id: %s', script_id, owner_id)
            return response
            
        except ClientError as e:
            log.exception('Failed to update releases. script_id: %s, owner_id: %s', script_id, owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to update releases')


    def __configure_dynamodb_resource(self) -> boto3.resources.factory.ServiceResource:
        """
        Configures and returns a DynamoDB service resource.

        Returns:
            boto3.resources.factory.ServiceResource: The DynamoDB service resource.
        """
        if self.aws_config.is_local:
            return boto3.resource('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url='http://localhost:8000')
        else:
            config = Config(region_name=self.aws_config.dynamodb_aws_region)
            return boto3.resource('dynamodb', config=config)
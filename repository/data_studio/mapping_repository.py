from dataclasses import asdict
import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key
from typing import List, Optional
from dacite import from_dict

from utils import Singleton, DataTypeUtils
from model import DataStudioMapping, DataStudioSaveMapping
from controller import common_controller as common_ctrl
from configuration import AppConfig, AWSConfig
from exception import ServiceException
from enums import ServiceStatus, DataStudioMappingStatus

log = common_ctrl.log


class DataStudioMappingRepository(metaclass=Singleton):


    def __init__(self, app_config:AppConfig, aws_config: AWSConfig) -> None:
        """
        Initialize the DataStudioMappingRepository with the AWS and App configurations.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.app_config = app_config
        self.aws_config = aws_config

        self.table = self.__configure_dynamodb()


    def get_active_mappings(self, owner_id: str) -> List[DataStudioMapping]:
        """
        Retrieve active data studio mappings for a specific owner.

        Args:
            owner_id (str): The ID of the owner whose active mappings are to be retrieved.

        Returns:
            List[DataStudioMapping]: A list of active mappings for the specified owner.
        """
        log.info('Retrieving data studio mappings. owner_id: %s', owner_id)
        try:
            response = self.table.query(
                IndexName=self.app_config.data_studio_mappings_gsi_name,
                KeyConditionExpression=Key('owner_id').eq(owner_id),
                FilterExpression=Attr('active').eq(True)
            )
            return [
                from_dict(DataStudioMapping, DataTypeUtils.convert_decimals_to_float_or_int(item))
                for item in response.get('Items', [])
            ]
        except ClientError as e:
            log.exception('Failed to retrieve data studio mappings. owner_id: %s', owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve data studio mappings')
        

    def get_mapping(self, owner_id: str, mapping_id: str) -> List[DataStudioMapping]:
        """
        Retrieve a specific data studio mapping for an owner.

        Args:
            owner_id (str): The ID of the owner of the mapping.
            mapping_id (str): The ID of the mapping to retrieve.

        Returns:
            List[DataStudioMapping]: A list containing the requested mapping.
        """
        log.info('Retrieving data studio mapping. mapping_id: %s, owner_id: %s', mapping_id, owner_id)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('id').eq(mapping_id),
                FilterExpression=Attr('owner_id').eq(owner_id)
            )

            return [
                from_dict(DataStudioMapping, DataTypeUtils.convert_decimals_to_float_or_int(item)) 
                for item in response.get('Items', [])
            ]
        except ClientError as e:
            log.exception('Failed to retrieve data studio mapping. mapping_id: %s, owner_id: %s', mapping_id, owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve data studio mapping')


    def create_mapping(self, mapping: DataStudioMapping) -> None:
        """
        Creates a new partial data studio mapping entry in the database.

        Args:
            mapping (DataStudioMapping): The data studio mapping object to be saved.

        Returns:
            DataStudioMapping: The created data studio mapping object.

        Raises:
            ServiceException: If there is an issue saving the mapping to the database.
        """
        log.info('Creating data studio mapping. mapping_id: %s, user_id: %s, owner_id: %s', mapping.id, mapping.created_by, mapping.owner_id)
        try:
            self.table.put_item(Item=asdict(mapping))
            log.info('Successfully created data studio mapping. mapping_id: %s, user_id: %s, owner_id: %s', mapping.id, mapping.created_by, mapping.owner_id)
        except ClientError as e:
            log.exception('Failed to create mapping. mapping_id: %s, user_id: %s, owner_id: %s', mapping.id, mapping.created_by, mapping.owner_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Couldn\'t create the mapping')


    def get_user_draft(self, owner_id: str, mapping_id: str, user_id: str) -> Optional[DataStudioMapping]:
        """
        Retrieves a draft data studio mapping for a specified user.

        Args:
            owner_id (str): The ID of the mapping owner.
            mapping_id (str): The ID of the mapping entry.
            user_id (str): The ID of the user requesting the draft.

        Returns:
            Optional[DataStudioMapping]: The draft mapping entry if found, or None.

        Raises:
            ServiceException: If an error occurs while retrieving the draft.
        """
        log.info('Retrieving user draft. owner_id: %s, mapping_id: %s, user_id: %s', owner_id, mapping_id, user_id)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('id').eq(mapping_id) & Key('revision').eq(user_id),
                FilterExpression=Attr('owner_id').eq(owner_id) & Attr('status').eq(DataStudioMappingStatus.DRAFT.value)
            )
            draft = response.get('Item', None)
            
            if not draft:
                return None
            return from_dict(DataStudioMapping, DataTypeUtils.convert_decimals_to_float_or_int(draft)) 
        except ClientError as e:
            log.exception('Failed to retrieve user draft. owner_id: %s, mapping_id: %s, user_id: %s', owner_id, mapping_id, user_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve user draft')
        

    def save_mapping(self, owner_id: str, id: str, revision: str,  mapping: DataStudioSaveMapping) -> None:
        """
        Updates an existing data studio mapping entry in the database.

        Args:
            owner_id (str): The ID of the mapping owner.
            id (str): The primary ID of the mapping entry.
            revision (str): The revision ID of the mapping entry.
            mapping (DataStudioSaveMapping): The mapping object containing updated values.

        Raises:
            ServiceException: If an error occurs while updating the mapping.
        """
        log.info('Updating data studio mapping draft. owner_id: %s, mapping_id: %s, revision_id: %s', owner_id, id, revision)
        try:
            mapping_dict = asdict(mapping)

            update_expression = "SET " + ", ".join(f"#{key} = :{key}" for key in mapping_dict)
            expression_attribute_names = {f"#{key}": key for key in mapping_dict}
            expression_attribute_values = {f":{key}": value for key, value in mapping_dict.items()}

            self.table.update_item(
                Key={
                    'id': id,
                    'revision': revision
                },
                UpdateExpression=update_expression,
                ConditionExpression='attribute_exists(id) AND attribute_exists(revision) AND owner_id = :owner_id',
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues={**expression_attribute_values, ':owner_id': owner_id}
            )
            log.info('Successfully updated data studio mapping draft. owner_id: %s, mapping_id: %s, revision_id: %s', owner_id, id, revision)
        except ClientError as e:
            log.exception('Failed to update mapping draft. owner_id: %s, mapping_id: %s, revision_id: %s', owner_id, id, revision)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Could not update the mapping draft')


    def __configure_dynamodb(self):
        """
        Configures and returns a DynamoDB table resource.

        Returns:
            boto3.resources.factory.ServiceResource: The DynamoDB table resource.
        """
        resource = None
        if self.aws_config.is_local:
            resource = boto3.resource('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url='http://localhost:8000')
        else:
            config = Config(region_name=self.aws_config.dynamodb_aws_region)
            resource = boto3.resource('dynamodb', config=config)

        return resource.Table(self.app_config.data_studio_mappings_table_name)
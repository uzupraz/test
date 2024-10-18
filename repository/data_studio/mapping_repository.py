from dataclasses import asdict
import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key
from typing import List
from dacite import from_dict

from utils import Singleton, DataTypeUtils
from model import DataStudioMapping
from controller import common_controller as common_ctrl
from configuration import AppConfig, AWSConfig
from exception import ServiceException
from enums import ServiceStatus

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
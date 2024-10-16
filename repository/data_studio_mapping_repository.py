import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key
from typing import List, Dict

from utils import Singleton
from controller import common_controller as common_ctrl
from configuration import AppConfig, AWSConfig
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class DataStudioMappingRepository(metaclass=Singleton):


    OWNER_ID_INDEX = "owner_id-index"


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


    def get_active_mappings(self, owner_id: str) -> List[Dict]:
        log.info('Retrieving data studio mappings. owner_id: %s', owner_id)
        try:
            response = self.table.query(
                IndexName=self.OWNER_ID_INDEX,
                KeyConditionExpression=Key('owner_id').eq(owner_id),
                FilterExpression=Attr('active').eq(True)
            )
            return response.get('Items', [])
        except ClientError as e:
            log.exception('Failed to retrieve data studio mappings. owner_id: %s', owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve data studio mappings')


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
        
        return resource.Table(self.app_config.data_studio_mapping_table_name)
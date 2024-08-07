import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError

from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

log = common_ctrl.log

class CustomerTableRepository(metaclass=Singleton):


    def __init__(self, app_config:AppConfig, aws_config:AWSConfig) -> None:
        """
        Initialize the CustomerTableRepository with the AWS and App configurations.

        DynamoDB Resource: The DynamoDB resource provides a higher-level interface that allows us to interact
        with DynamoDB in an object-oriented manner. It simplifies operations such as creating tables, querying, updating items, etc.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.aws_config = aws_config
        self.app_config = app_config
        self.dynamodb_resource = self.__configure_dynamodb_resource()


    def get_table_items(self, table_name:str, limit:int, exclusive_start_key:dict=None) -> tuple[list, str|None]:
        """
        Retrieve items from a DynamoDB table with pagination.

        Args:
            table_name (str): The name of the DynamoDB table from which to retrieve items.
            limit (int): The maximum number of items to retrieve.
            exclusive_start_key (dict, optional): The key to start retrieving items from. If not provided, starts from the beginning.

        Returns:
            tuple: A tuple containing:
                - A list of retrieved items (list of dicts).
                - The last evaluated key for pagination (dict or None).
        
        Raises:
            ServiceException: If there is an issue retrieving items from the DynamoDB table.

        Notes:
            - If `exclusive_start_key` is provided, the method will start scanning from that key, which is used for pagination.
            - The `response.get('LastEvaluatedKey', None)` is used to determine if there are more items to be retrieved.
        """
        log.info('Retrieving table items. table_name: %s', table_name)
        try:
            table = self.dynamodb_resource.Table(table_name)
            params = {
                'Limit': limit,
            }
            if exclusive_start_key:
                params['ExclusiveStartKey'] = exclusive_start_key

            response = table.scan(**params)
            log.info('Successfully retrieved table items. table_name: %s', table_name)
            return response.get('Items', []), response.get('LastEvaluatedKey', None)
        except ClientError:
            log.exception('Failed to retrieve table items. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve table items')
      

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

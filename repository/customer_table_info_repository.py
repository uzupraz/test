import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

log = common_ctrl.log

class CustomerTableInfoRepository(metaclass=Singleton):


    def __init__(self, app_config:AppConfig, aws_config:AWSConfig) -> None:
        """
        Initialize the CustomerTableInfoRepository with the AWS and App configurations.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.aws_config = aws_config
        self.app_config = app_config
        self.dynamodb = self.__configure_dynamodb_resource()
        self.dynamodb_client = self.__configure_dynamodb_client()
        self.table = self.__configure_table()


    def get_tables_for_owner(self, owner_id:str) -> list[dict[str, any]]:
        """
        Get a list of tables for a particular owner_id.

        Args:
            owner_id (str): The owner ID to query the table.

        Returns:
            List[Dict[str, Any]]: A list of table dictionaries containing table details for the specified owner_id.

        Raises:
            ServiceException: If owner_id is null or empty or if there is an error querying the DynamoDB table.
        """
        log.info('Retrieving table details. owner_id: %s', owner_id)
        if not owner_id:
            raise ServiceException(500, ServiceStatus.FAILURE, 'owner_id cannot be null or empty')

        try:
            response = self.table.query(
                KeyConditionExpression=Key('owner_id').eq(owner_id)
            )
            log.info('Successfully retrieved table details. owner_id: %s', owner_id)
            return response.get('Items', [])
        except ClientError as e:
            log.exception('Failed to retrieve table details. owner_id: %s', owner_id)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, e.response['Error']['Message'])


    def get_table_size(self, table_name:str) -> int:
        """
        Get the size of a specific table.

        Args:
            table_name (str): The name of the table to retrieve size for.

        Returns:
            int: The table size in kilobytes.

        Raises:
            ServiceException: If there is an error describing the DynamoDB table.
        """
        try:
            log.info('Retrieving the size of table. table_name: %s', table_name)
            response = self.dynamodb_client.describe_table(TableName=table_name)
            log.info('Successfully retrieved the size of table. table_name: %s', table_name)
            return response['Table'] ['TableSizeBytes'] // 1024  # Convert bytes to kilobytes
        except ClientError as e:
            log.exception('Failed to retrieve the size of table. table_name: %s', table_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, e.response['Error']['Message'])


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


    def __configure_dynamodb_client(self) -> boto3.client:
        """
        Configures and returns a DynamoDB client.

        Returns:
            boto3.client: The DynamoDB client.
        """
        if self.aws_config.is_local:
            return boto3.client('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url='http://localhost:8000')
        else:
            config = Config(region_name=self.aws_config.dynamodb_aws_region)
            return boto3.client('dynamodb', config=config)


    def __configure_table(self):
        """
        Configures and returns the DynamoDB table object.

        Returns:
            boto3.resources.factory.dynamodb.Table: The DynamoDB table object.
        """
        return self.dynamodb.Table(self.app_config.customer_table_info_table_name)

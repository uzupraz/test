import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import dacite

from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from model import CustomerTableInfo
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

log = common_ctrl.log

class CustomerTableInfoRepository(metaclass=Singleton):


    def __init__(self, app_config:AppConfig, aws_config:AWSConfig) -> None:
        """
        Initialize the CustomerTableInfoRepository with the AWS and App configurations.

        DynamoDB Resource: The DynamoDB resource provides a higher-level interface that allows us to interact
        with DynamoDB in an object-oriented manner. It simplifies operations such as creating tables, querying, updating items, etc.

        DynamoDB Client: The DynamoDB client provides a lower-level interface that allows for direct interaction with
        DynamoDB through API calls. This is particularly useful for operations that are not directly supported by
        the resource interface, such as the describe_table operation used in the 'get_table_size' method.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.aws_config = aws_config
        self.app_config = app_config
        self.dynamodb_resource = self.__configure_dynamodb_resource()
        self.dynamodb_client = self.__configure_dynamodb_client()
        self.table = self.__configure_table()


    def get_tables_for_owner(self, owner_id:str) -> list[CustomerTableInfo]:
        """
        Get a list of tables for a particular owner_id.

        Args:
            owner_id (str): The owner ID to query the table.

        Returns:
            list[CustomerTableInfo]: List of table details for the specified owner_id.

        Raises:
            ServiceException: If there is an error querying the DynamoDB table.
        """
        log.info('Retrieving all tables. owner_id: %s', owner_id)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('owner_id').eq(owner_id)
            )
            log.info('Successfully retrieved all tables. owner_id: %s', owner_id)
            customer_info_tables = []
            for item in response.get('Items', []):
                customer_info_table = dacite.from_dict(CustomerTableInfo, item)
                customer_info_tables.append(customer_info_table)
            return customer_info_tables
        except ClientError as e:
            log.exception('Failed to retrieve all tables. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve tables')


    def get_table_details(self, table_name:str) -> dict:
        """
        Get the details of a specific table.

        Args:
            table_name (str): The name of the table to retrieve details for.

        Returns:
            dict: The response from the describe_table API.

        Raises:
            ServiceException: If there is an error describing the DynamoDB table.
        """
        try:
            log.info('Retrieving details of table. table_name: %s', table_name)
            response = self.dynamodb_client.describe_table(TableName=table_name)
            log.info('Successfully retrieved details of table. table_name: %s', table_name)
            return response
        except ClientError as e:
            log.exception('Failed to retrieve details of table. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve table details')


    def update_table(self, owner_id:str, table_id:str, description:str) -> None:
        """
        Updates the fields of a specified table of particular owner.

        Args:
            owner_id (str): The owner of the table.
            table_id (str): The ID of the table.
            description (str): The description to update in the table.

        Raises:
            ServiceException: If there is an error, updating the DynamoDB table.
        """
        try:
            log.info('Updating table. owner_id: %s, table_id: %s', owner_id, table_id)
            table_key = {'owner_id': owner_id, 'table_id': table_id}
            update_expression = 'SET description = :desc'
            expression_attribute_values = {':desc': description}
            self.table.update_item(
                Key=table_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )
            log.info('Successfully updated table. owner_id: %s, table_id: %s', owner_id, table_id)
        except ClientError as e:
            log.exception('Failed to update table. owner_id: %s, table_id: %s', owner_id, table_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to update description of table.')


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
        return self.dynamodb_resource.Table(self.app_config.customer_table_info_table_name)

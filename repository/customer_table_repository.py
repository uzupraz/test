import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

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


    def get_table_items(self, table_name:str, limit:int, exclusive_start_key:dict=None) -> tuple[list, dict|None]:
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
    
    
    def create_item(self, table_name: str, item: dict[str, any]) -> dict[str,any]:
        """
        Creates an item into the specified DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table.
            item (dict[str, any]): The item data to insert into the table.

        Returns:
            dict[str, any]: The inserted item.

        Raises:
            ServiceException: If there is an issue inserting the item into the DynamoDB table.
        """
        log.info('Inserting item into table. table_name: %s, item: %s', table_name, item)
        try:
            table = self.dynamodb_resource.Table(table_name)
            table.put_item(Item=item)
            log.info('Successfully inserted item into table. table_name: %s', table_name)
            return item
        except ClientError:
            log.exception('Failed to insert item into table. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to insert item into table')
        
    
    def delete_item(self, table_name: str, key: dict[str, any]) -> None:
        """
        Deletes an item from the specified DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table.
            key (dict[str, any]): The key of the item to delete. This should match the table's primary key schema.

        Raises:
            ServiceException: If there is an issue deleting the item from the DynamoDB table.
        """
        log.info('Deleting item from table. table_name: %s, key: %s', table_name, key)
        try:
            table = self.dynamodb_resource.Table(table_name)
            table.delete_item(Key=key)
            log.info('Successfully deleted item from table. table_name: %s', table_name)
        except ClientError:
            log.exception('Failed to delete item from table. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to delete item from table')
        

    def query_item(self, table_name: str, partition: tuple[str,str], sort: tuple[str,str] | None, filters: dict[str, any] = None) -> dict:
        """
        Queries an item from the specified DynamoDB table using partition and sort keys, with optional filters.

        Args:
            table_name (str): The name of the DynamoDB table.
            partition_key (str): The partition key of the item to query.
            sort_key (str): The sort key of the item to query.
            filters (dict[str, any]): Optional. A dictionary of additional attributes to filter by.

        Returns:
            dict: The queried items.

        Raises:
            ServiceException: If there is an issue querying the item from the DynamoDB table.
        """
        log.info('Querying item from table. table_name: %s, partition_key: %s, sort_key: %s, filters: %s', table_name, partition, sort, filters)
        try:
            partition_key, partition_key_value = partition

            table = self.dynamodb_resource.Table(table_name)
            key_condition = Key(partition_key).eq(partition_key_value)

            if sort is not None:
                sort_key, sort_key_value = sort
                key_condition &= Key(sort_key).eq(sort_key_value)

            # Prepare filter expression if filters are provided
            filter_expression = None
            if filters:
                for k, v in filters.items():
                    if filter_expression is None:
                        filter_expression = Attr(k).eq(v)
                    else:
                        filter_expression &= Attr(k).eq(v)

            query_params = {
                'KeyConditionExpression': key_condition,
            }
            if filter_expression:
                query_params['FilterExpression'] = filter_expression

            response = table.query(**query_params)

            items = response.get('Items', [])
            log.info('Successfully queried item from table. table_name: %s', table_name)
            return items
        except ClientError:
            log.exception('Failed to query item from table. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to query item from table')


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

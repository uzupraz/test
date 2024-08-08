import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dacite import from_dict

from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from model import CustomerTableInfo, BackupDetail
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

        DynamoDB Backup Client: The DynamoDB provides a low-level client representing AWS Backup, which simplifies the creation,
        migration, restoration, and deletion of backups, etc.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.aws_config = aws_config
        self.app_config = app_config
        self.dynamodb_resource = self.__configure_dynamodb_resource()
        self.dynamodb_client = self.__configure_dynamodb_client()
        self.dynamodb_backup_client = self.__configure_backup_client()
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
        log.info('Retrieving customer tables. owner_id: %s', owner_id)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('owner_id').eq(owner_id)
            )
            log.info('Successfully retrieved customer tables. owner_id: %s', owner_id)
            customer_info_tables = []
            for item in response.get('Items', []):
                customer_info_tables.append(from_dict(CustomerTableInfo, item))
            return customer_info_tables
        except ClientError as e:
            log.exception('Failed to retrieve customer tables. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve customer tables')


    def get_table_size(self, table_name:str) -> float:
        """
        Get the size of a specific dynamoDB table.

        Args:
            table_name (str): The name of the dynamoDB table to retrieve size for.

        Returns:
            float: The table size.

        Raises:
            ServiceException: If there is an error describing the DynamoDB table.
        """
        try:
            log.info('Retrieving size of customer table. table_name: %s', table_name)
            response = self.dynamodb_client.describe_table(TableName=table_name)
            log.info('Successfully retrieved size of customer table. table_name: %s', table_name)
            return response['Table']['TableSizeBytes'] / 1024
        except ClientError as e:
            log.exception('Failed to retrieve size of customer table. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve size of customer table')


    def get_table_item(self, owner_id:str, table_id:str) -> CustomerTableInfo:
        """
        Retrieves customer's table item based on owner_id and table_id.

        Args:
            owner_id (str): The ID of the owner.
            table_id (str): The ID of the table.

        Returns:
            CustomerTableInfo: The retrieved item as customer table info object.

        Raises:
            ServiceException: If the item does not exists or if there is an error querying the DynamoDB table.
        """
        try:
            log.info('Retrieving customer table item. owner_id: %s, table_id: %s', owner_id, table_id)
            response = self.table.get_item(
                Key={'owner_id': owner_id, 'table_id': table_id}
            )
            item = response.get('Item')
            if not item:
                log.error('Customer table item does not exist. owner_id: %s, table_id: %s', owner_id, table_id)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Customer table item does not exists')
            log.info('Successfully retrieved customer table item. owner_id: %s, table_id: %s', owner_id, table_id)
            return from_dict(CustomerTableInfo, item)
        except ClientError as e:
            log.exception('Failed to retrieve customer table item. owner_id: %s, table_id: %s', owner_id, table_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve customer table item')


    def update_description(self, customer_table_info:CustomerTableInfo) -> CustomerTableInfo:
        """
        Updates the description field of a customer's table.

        Args:
            customer_table_info (CustomerTableInfo): The customer table info with data to update.

        Returns:
            CustomerTableInfo: The updated customer table info.

        Raises:
            ServiceException: If there is an error, updating the DynamoDB table.
        """
        try:
            log.info('Updating customer table description. owner_id: %s, table_id: %s', customer_table_info.owner_id, customer_table_info.table_id)
            table_key = {'owner_id': customer_table_info.owner_id, 'table_id': customer_table_info.table_id}
            update_expression = 'SET description = :desc'
            expression_attribute_values = {':desc': customer_table_info.description}
            response = self.table.update_item(
                Key=table_key,
                UpdateExpression=update_expression,
                ConditionExpression=Attr('owner_id').exists() & Attr('table_id').exists(),
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="ALL_NEW"
            )
            log.info('Successfully updated customer table description. owner_id: %s, table_id: %s', customer_table_info.owner_id, customer_table_info.table_id)
            return from_dict(CustomerTableInfo, response.get('Attributes'))
        except ClientError as e:
            log.exception('Failed to update customer table description. owner_id: %s, table_id: %s', customer_table_info.owner_id, customer_table_info.table_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to update customer table description')


    def get_table_backup_details(self, table_name:str, table_arn:str) -> list[BackupDetail]:
        """
        Get the backup details of a specific DynamoDB table.

        Args:
            table_name (str): The name of the DynamoDB table to retrieve backup details for.
            table_arn (str): The Amazon Resource Name (ARN) of the DynamoDB table to retrieve backup details for.

        Returns:
            list[BackupDetail]: The backup details of dynamoDB table.

        Raises:
            ServiceException: If there is an error, retrieving the backup details of dynamoDB table.
        """
        try:
            log.info('Retrieving backup details of customer table. table_name: %s', table_name)
            response = self.dynamodb_backup_client.list_backup_jobs(ByResourceArn=table_arn)
            backup_details = [
                BackupDetail(id=backup_job['BackupJobId'],
                             name=table_name + '_' + backup_job['CreationDate'].strftime('%Y%m%d%H%M%S'),
                             creation_time=backup_job['CreationDate'].strftime('%Y-%m-%d %H:%M:%S%z'),
                             size=backup_job['BackupSizeInBytes'] / 1024)
                # the response contains the list of BackupJob i.e. response ={'BackupJobs': [{details}]}
                for backup_job in response['BackupJobs']
            ]
            log.info('Successfully retrieved backup details of customer table. table_name: %s', table_name)
            return backup_details
        except ClientError as e:
            log.exception('Failed to retrieve backup details of customer table. table_name: %s', table_name)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrieve backup details of customer table')


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


    def __configure_backup_client(self) -> boto3.client:
        """
        Configures and returns a DynamoDB backup client.

        Returns:
            boto3.client: The DynamoDB backup client.
        """
        config = Config(region_name=self.aws_config.dynamodb_aws_region)
        return boto3.client('backup', config=config)


    def __configure_table(self):
        """
        Configures and returns the DynamoDB table object.

        Returns:
            boto3.resources.factory.dynamodb.Table: The DynamoDB table object.
        """
        return self.dynamodb_resource.Table(self.app_config.customer_table_info_table_name)

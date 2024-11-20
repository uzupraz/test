import boto3
import boto3.resources
import boto3.resources.factory
from botocore.config import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from typing import List, Optional
from dacite import from_dict

from utils import Singleton, DataTypeUtils
from model import DataFormat
from controller import common_controller as common_ctrl
from configuration import AppConfig, AWSConfig
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log


class DataFormatsRepository(metaclass=Singleton):


    def __init__(self, app_config:AppConfig, aws_config: AWSConfig) -> None:
        """
        Initialize the DataFormatsRepository with the AWS and App configurations.

        Args:
            app_config (AppConfig): The application configuration object.
            aws_config (AWSConfig): The AWS configuration object.
        """
        self.app_config = app_config
        self.aws_config = aws_config

        self.table = self.__configure_dynamodb()


    def list_all_data_formats(self) -> List[DataFormat]:
        """
        Retrieve all data formats from the DynamoDB table.

        Returns:
            List[DataFormat]: A list of DataFormat objects retrieved from the DynamoDB table.

        Raises:
            ServiceException: If there is an issue retrieving data formats from the DynamoDB table.
        """
        log.info('Retrieving data formats')
        try:
            response = self.table.scan()
            return [
                from_dict(DataFormat, DataTypeUtils.convert_decimals_to_float_or_int(item))
                for item in response.get('Items', [])
            ]
        except ClientError as e:
            log.exception('Error while retrieving data formats')
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Error while retrieving data formats')
        

    def get_data_format(self, format_name: str) -> Optional[DataFormat]:
        """
        Retrieve data format from the DynamoDB table for the given format.

        Args:
            format_name (str): The name of the data format.

        Returns:
            Optional[DataFormat]: A DataFormat objects retrieved from the DynamoDB table.

        Raises:
            ServiceException: If there is an issue retrieving data format from the DynamoDB table.
        """
        log.info('Retrieving data format. format: %s', format)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('format_name').eq(format_name)
            )
            formats = response.get('Items', [])
            if not formats:
                log.error('Unable to find data format. format_name: %s', format_name)
                return None
            
            return from_dict(DataFormat, DataTypeUtils.convert_decimals_to_float_or_int(formats[0]))
        except ClientError as e:
            log.exception('Error while retrieving data format. format_name: %s', format_name)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Error while retrieving data format')


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

        return resource.Table(self.app_config.data_formats_table_name)
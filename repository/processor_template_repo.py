import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import List
import dacite

from model import ProcessorTemplate
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils.helper_types import Singleton

log = common_ctrl.log


class ProcessorTemplateRepo(Singleton):


    _instance = None


    def __init__(self, app_config:AppConfig, aws_config:AWSConfig) -> None:
        self.aws_config = aws_config
        self.app_config = app_config
        self.table = self.__configure_table()


    def get_all_templates(self) -> List[ProcessorTemplate]:

        templates = []
        try:
            response = self.table.scan()
            for item in response.get('Items', []):
                template = dacite.from_dict()
                templates.append(template)
        except ClientError as e:
            log.exception('Failed to list all templates. Result: ', e.response['ResponseMetadata']['HTTPStatusCode'])
            raise ServiceException(500, ServiceStatus.FAILURE, 'Could not load available templates list')
        return templates


    def __configure_table(self) -> boto3.Tabl:
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.
        """
        dynamo_db = None

        if self.aws_config.is_local:
            dynamo_db = boto3.resource('dynamodb', region_name=self.aws_config.aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.aws_region)
            dynamo_db = boto3.resource('dynamodb', config = config)

        return dynamo_db.Table(self.app_config.processor_templates_table_name)

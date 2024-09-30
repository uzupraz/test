import boto3
from botocore.config import Config
from botocore.exceptions import ClientError 
from boto3.dynamodb.conditions import Key
from typing import List

from model import Module
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

log = common_ctrl.log

class UpdaterRepository(metaclass=Singleton):


    def __init__(self, app_config: AppConfig, aws_config: AWSConfig) -> None:
        self.aws_config = aws_config
        self.app_config = app_config
        self.release_table = self.__configure_table(self.app_config.updater_release_table_name)
        self.csa_info_table = self.__configure_table(self.app_config.updater_csa_info_table_name)

    def get_csa_info_table_items(self,owner_id:str, machine_id: str):
        table_name=self.app_config.updater_csa_info_table_name
        log.info('Retrieving table items. table_name: %s', table_name)
        try:
            response = self.csa_info_table.query(
                KeyConditionExpression=Key('owner_id').eq(owner_id) & Key('machine_id').eq(machine_id),
                Limit=1
            )
            log.info('Successfully retrieved table items. table_name: %s', table_name)
            return response.get('Items',[])
        except ClientError as e:
            log.exception('Failed to retrive table items. table_name: %s, status_code: %s, message: %s', table_name, e.response['ResponseMetadata']['HTTPStatusCode'], e.response['Error']['Message'])
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrive table items')
        

    def get_release_info(self, module_name: str):
        table_name=self.app_config.updater_release_table_name
        try:
            response = self.release_table.query(
                KeyConditionExpression=Key('module_name').eq(module_name),
                ScanIndexForward=False,
            )
            log.info('Successfully retrieved table items. table_name: %s', table_name)
            return response.get('Items',[])
        except ClientError as e:
            log.exception('Failed to retrive table items. table_name: %s, status_code: %s, message: %s', table_name, e.response['ResponseMetadata']['HTTPStatusCode'], e.response['Error']['Message'])
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to retrive table items')
        

    def update_module_version_list(self, machine_id: str, owner_id: str, module_version_list: List[Module]):
        try:
            log.info('Updating module_version list. owner_id: %s, machine_id: %s', owner_id, machine_id)
            self.csa_info_table.update_item(
                Key={'machine_id': machine_id, 'owner_id': owner_id},
                UpdateExpression="SET module_version_list = :updateList",
                ExpressionAttributeValues={':updateList': module_version_list}
            )
            log.info('Successfully updated module_version list. owner_id: %s, machine_id: %s', owner_id, machine_id)
        except ClientError as e:
            log.exception('Failed to update module_version list. owner_id: %s, machine_id: %s', owner_id, machine_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to update module_version list description')

    
    def __configure_table(self, table_name: str):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.
        """
        dynamo_db = None

        if self.aws_config.is_local:
            dynamo_db = boto3.resource('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.dynamodb_aws_region)
            dynamo_db = boto3.resource('dynamodb', config = config)

        return dynamo_db.Table(table_name)
    
        
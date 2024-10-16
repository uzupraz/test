import boto3
from botocore.config import Config
from botocore.exceptions import ClientError 
from boto3.dynamodb.conditions import Key
from dacite import from_dict
from typing import List

from model import Module, MachineInfo, ModuleInfo
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton, DataTypeUtils

log = common_ctrl.log


class CsaMachinesRepository(metaclass=Singleton):


    def __init__(self, app_config: AppConfig, aws_config: AWSConfig) -> None:
        """
        Initializes the repository with application and AWS configurations.

        Args:
            app_config (AppConfig): Application configuration containing table names.
            aws_config (AWSConfig): AWS configuration for DynamoDB connection.
        """
        self.aws_config = aws_config
        self.app_config = app_config
        
        self.table = self.__configure_dynamodb()


    def get_csa_machine_info(self, owner_id:str, machine_id: str) -> MachineInfo:
        """
        Retrieves machine information based on owner ID and machine ID.

        Args:
            owner_id (str): The ID of the owner.
            machine_id (str): The ID of the machine.

        Returns:
            MachineInfo: A MachineInfo objects corresponding to the specified owner and machine ID.

        Raises:
            ServiceException: If the retrieval of machine information fails.
        """
        log.info('Retrieving machine information. owner_id: %s, machine_id: %s', owner_id, machine_id)
        try:
            response = self.table.get_item(
                Key={'owner_id': owner_id, 'machine_id': machine_id}
            )
            item = response.get('Item')
            if not item:
                log.error('Machine info does not exist. owner_id: %s, machine_id: %s', owner_id, machine_id)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Machine info does not exists')
            
            log.info('Successfully retrieved machine info. owner_id: %s, machine_id: %s', owner_id, machine_id)
            return from_dict(MachineInfo, item)

        except ClientError as e:
            log.exception("Failed to retrieve owner's machine information. owner_id: %s, machine_id: %s", owner_id, machine_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, "Could not retrieve owner's machine info")
        
        

    def update_modules(self, owner_id: str, machine_id: str, modules: List[Module]) -> None:
        """
        Updates the modules for a specified owner ID and machine ID.

        Args:
            owner_id (str): The ID of the owner.
            machine_id (str): The ID of the machine.
            modules (List[Module]): A list of Module objects to be updated.

        Raises:
            ServiceException: If the update of modules fails.
        """
        try:
            log.info('Updating modules. owner_id: %s, machine_id: %s', owner_id, machine_id)
            self.table.update_item(
                Key={'owner_id': owner_id, 'machine_id': machine_id},
                UpdateExpression="SET modules = :updateList",
                ExpressionAttributeValues={':updateList': modules}
            )
            log.info('Successfully updated modules. owner_id: %s, machine_id: %s', owner_id, machine_id)
        except ClientError as e:
            log.exception('Failed to update modules. owner_id: %s, machine_id: %s', owner_id, machine_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to update modules')

    
    def __configure_dynamodb(self):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.`
        """
        resource = None

        if self.aws_config.is_local:
            resource = boto3.resource('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.dynamodb_aws_region)
            resource = boto3.resource('dynamodb', config = config)

        return resource.Table(self.app_config.csa_machines_table_name)
    
        
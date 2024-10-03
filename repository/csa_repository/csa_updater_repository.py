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
from utils import Singleton

log = common_ctrl.log

class CsaUpdaterRepository(metaclass=Singleton):


    def __init__(self, app_config: AppConfig, aws_config: AWSConfig) -> None:
        """
        Initializes the repository with application and AWS configurations.

        Args:
            app_config (AppConfig): Application configuration containing table names.
            aws_config (AWSConfig): AWS configuration for DynamoDB connection.
        """
        self.aws_config = aws_config
        self.app_config = app_config
        self.csa_module_versions_table = self.__configure_table(self.app_config.updater_csa_module_versions_table_name)
        self.csa_machines_table = self.__configure_table(self.app_config.updater_csa_machines_table_name)


    def get_csa_machines_info(self, owner_id:str, machine_id: str) -> List[MachineInfo]:
        """
        Retrieves machine information based on owner ID and machine ID.

        Args:
            owner_id (str): The ID of the owner.
            machine_id (str): The ID of the machine.

        Returns:
            List[MachineInfo]: A list of MachineInfo objects corresponding to the specified owner and machine ID.

        Raises:
            ServiceException: If the retrieval of machine information fails.
        """
        table_name=self.app_config.updater_csa_machines_table_name
        log.info('Retrieving machine information. table_name: %s and owner_id: %s', table_name, owner_id)
        try:
            response = self.csa_machines_table.query(
                KeyConditionExpression=Key('owner_id').eq(owner_id) & Key('machine_id').eq(machine_id),
                Limit=1
            )
            items = response.get('Items')
            csa_machine_info = []
            for item in items:
                csa_machine_info.append(from_dict(MachineInfo, item))

        except ClientError as e:
            log.exception("Failed to retrieve owner's machine information. table_name: %s, owner_id: %s, message: %s", table_name, owner_id, e.response['Error']['Message'])
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, "Could not retrieve owner's machine info")
        return csa_machine_info
        

    def get_csa_module_versions(self, module_name: str) -> List[ModuleInfo]:
        """
        Retrieves the latest module versions for a given module name.

        Args:
            module_name (str): The name of the module to retrieve versions for.

        Returns:
            List[ModuleInfo]: A list of ModuleInfo objects containing the latest module versions.

        Raises:
            ServiceException: If the retrieval of module versions fails.
        """
        table_name=self.app_config.updater_csa_module_versions_table_name
        log.info('Retrieving latest modules. table name: %s', table_name)
        try:
            response = self.csa_module_versions_table.query(
                KeyConditionExpression=Key('module_name').eq(module_name),
                ScanIndexForward=False,
            )
            items = response.get('Items',[])
            latest_modules = []
            for item in items:
                latest_modules.append(from_dict(ModuleInfo, item))
        except ClientError as e:
            log.exception("Failed to retrieve latest modules. table_name: %s, message: %s", table_name, e.response['Error']['Message'])
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, "Could not retrieve owner's machine info")
        return latest_modules
        

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
            self.csa_machines_table.update_item(
                Key={'owner_id': owner_id, 'machine_id': machine_id},
                UpdateExpression="SET modules = :updateList",
                ExpressionAttributeValues={':updateList': modules}
            )
            log.info('Successfully updated modules. owner_id: %s, machine_id: %s', owner_id, machine_id)
        except ClientError as e:
            log.exception('Failed to update modules. owner_id: %s, machine_id: %s', owner_id, machine_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to update modules')

    
    def __configure_table(self, table_name: str):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.`
        """
        dynamo_db = None

        if self.aws_config.is_local:
            dynamo_db = boto3.resource('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.dynamodb_aws_region)
            dynamo_db = boto3.resource('dynamodb', config = config)

        return dynamo_db.Table(table_name)
    
        
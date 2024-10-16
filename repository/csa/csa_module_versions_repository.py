import boto3
from botocore.config import Config
from botocore.exceptions import ClientError 
from boto3.dynamodb.conditions import Key
from dacite import from_dict
from typing import List

from model import ModuleInfo
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

log = common_ctrl.log


class CsaModuleVersionsRepository(metaclass=Singleton):


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


    def get_csa_module_versions(self, module_name: str) -> List[ModuleInfo]:
        """
        Retrieves the latest module versions for a given module name.

        Args:
            module_name (str): The name of the module to retrieve versions for.

        Returns:
            List[ModuleInfo]: A list of ModuleInfo objects containing the latest module versions.

        Raises:
            ServiceException: If the retrieval of module fails.
        """
        log.info('Retrieving modules. module_name: %s', module_name)
        try:
            response = self.table.query(
                KeyConditionExpression=Key('module_name').eq(module_name)
            )
            items = response.get('Items',[])
            latest_modules = []
            for item in items:
                latest_modules.append(from_dict(ModuleInfo, item))
        except ClientError as e:
            log.exception("Failed to retrieve modules. module_name: %s", module_name)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, "Could not retrieve modules")
        
        return latest_modules
        

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

        return resource.Table(self.app_config.csa_module_versions_table_name)
    
    
        
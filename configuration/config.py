import os
import dataclasses

from utils import Singleton


@dataclasses.dataclass(init=False)
class AppConfig(metaclass=Singleton):
    """
    Configuration required for retrieving the os or local environment variables
    Any environment variables to be used in the application should be set here
    """
    log_level:str = os.getenv('APP_LOG_LEVEL', 'DEBUG').upper()
    workflow_table_name:str = os.getenv('APP_WORKFLOW_TABLENAME')
    processor_templates_table_name:str = os.getenv('APP_PROCESSORTEMPLATES_TABLENAME')


@dataclasses.dataclass(init=False)
class AWSConfig(metaclass=Singleton):
    """
    Configuration related to the AWS are loaded here.
    """
    is_local: bool = os.getenv('AWS_IS_LOCAL', 'False').lower() == 'true'
    aws_region: str = os.getenv('AWS_REGION')


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
    customer_table_info_table_name:str = os.getenv('APP_CUSTOMER_TABLE_INFO_TABLENAME')
    custom_script_table_name:str = os.getenv('APP_CUSTOM_SCRIPT_TABLENAME')
    data_studio_mappings_table_name:str = os.getenv('APP_DATA_STUDIO_MAPPINGS_TABLENAME')
    data_formats_table_name:str = os.getenv('APP_DATA_FORMATS_TABLENAME')
    processor_templates_table_name:str = os.getenv('APP_PROCESSORTEMPLATES_TABLENAME')
    csa_module_versions_table_name: str = os.getenv('CSA_MODULE_VERSIONS_TABLENAME')
    csa_machines_table_name: str = os.getenv('CSA_MACHINES_TABLENAME')
    data_studio_mappings_gsi_name:str = os.getenv('APP_DATA_STUDIO_MAPPINGS_GSI_NAME')


@dataclasses.dataclass(init=False)
class AWSConfig(metaclass=Singleton):
    """
    Configuration related to the AWS are loaded here.
    """
    is_local: bool = os.getenv('AWS_IS_LOCAL', 'False').lower() == 'true'
    dynamodb_aws_region: str = os.getenv('AWS_DYNAMODB_REGION')
    stepfunctions_execution_role_arn: str = os.getenv('AWS_STEP_FUNCTION_EXECUTION_ROLE_ARN')
    sqs_billing_arn: str = os.getenv('AWS_SQS_BILLING_ARN')


@dataclasses.dataclass(init=False)
class AsyncFileDeliveryS3Config(metaclass=Singleton):
    """
    Configuration needed for async file delivery and download are loaded here.
    """
    input_bucket_name: str = os.getenv('APP_FILES_INPUT_BUCKET_NAME')
    output_bucket_name: str = os.getenv('APP_FILES_OUTPUT_BUCKET_NAME')
    archive_bucket_name: str = os.getenv('APP_FILES_ARCHIVE_BUCKET_NAME')
    object_prefix: str = os.getenv('APP_FILES_OBJECT_PREFIX', '')
    pre_signed_url_expiration: int = int(os.getenv('APP_FILES_PRE_SIGNED_URL_EXPIRATION_IN_SECONDS', 3600))

@dataclasses.dataclass(init=False)
class S3AssetsFileConfig(metaclass=Singleton):
    """
    Configuration needed for assets bucket are loaded here.
    """
    assets_bucket_name: str = os.getenv('S3_ASSETS_BUCKET_NAME')
    pre_signed_url_expiration: int = int(os.getenv('S3_ASSETS_PRE_SIGNED_URL_EXPIRATION_IN_SECONDS',3600))


@dataclasses.dataclass(init=False)
class OpensearchConfig(metaclass=Singleton):
    """
    Configuration related to the Opensearch are loaded here.
    """
    host:str = os.getenv('OPENSEARCH_HOST')
    region:str = os.getenv('OPENSEARCH_REGION')
    service:str = os.getenv('OPENSEARCH_SERVICE', 'es')
    port:int = int(os.getenv('OPENSEARCH_PORT', 443))
    pool_maxsize:int = int(os.getenv('OPENSEARCH_POOL_MAXSIZE', 20))
    index:str = os.getenv('OPENSEARCH_INDEX')
    timeout:int = int(os.getenv('OPENSEARCH_TIMEOUT_IN_SECONDS', 30))

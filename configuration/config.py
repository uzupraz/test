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
    chatbot_messages_table_name: str = os.getenv('APP_CHATBOT_MESSAGES_TABLENAME')
    chatbot_messages_gsi_name:str = os.getenv('APP_CHATBOT_MESSAGES_GSI_NAME')


@dataclasses.dataclass(init=False)
class AWSConfig(metaclass=Singleton):
    """
    Configuration related to the AWS are loaded here.
    """
    is_local: bool = os.getenv('AWS_IS_LOCAL', 'False').lower() == 'true'
    dynamodb_aws_region: str = os.getenv('AWS_DYNAMODB_REGION')

    stepfunction_execution_role_arn: str = os.getenv('AWS_STEP_FUNCTION_EXECUTION_ROLE_ARN')
    sqs_workflow_billing_arn: str = os.getenv('AWS_SQS_WORKFLOW_BILLING_ARN')
    cloudwatch_log_group_base: str = os.getenv('AWS_CLOUD_WATCH_LOG_GROUP_BASE')
    cloudwatch_retention_in_days = os.getenv('AWS_CLOUD_WATCH_RETENTION_IN_DAYS')
    json_transformer_processor_arn = os.getenv('AWS_JSON_TRANSFORMER_PROCESSOR_ARN')


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


@dataclasses.dataclass(init=False)
class AwsBedrockConfig(metaclass=Singleton):
    """
    Configuration related to the Aws Bedrock are loaded here.
    """
    model_id:str = os.getenv('AWS_BEDROCK_MODEL_ID', 'anthropic.claude-3-haiku-20240307-v1:0')
    anthropic_version:str = os.getenv('AWS_BEDROCK_ANTHROPIC_VERSION', 'bedrock-2023-05-31')
    max_tokens:int = int(os.getenv('AWS_BEDROCK_MAX_TOKENS', 1000))


@dataclasses.dataclass(init=False)
class PostgresConfig:
    postgres_host:str = os.getenv('POSTGRES_HOST')
    postgres_port:int = os.getenv('POSTGRES_PORT')
    postgres_user:str = os.getenv('POSTGRES_USER')
    postgres_pass:str = os.getenv('POSTGRES_PASS')
    postgres_database:str = os.getenv('POSTGRES_DATABASE')
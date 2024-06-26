import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from controller import common_controller as common_ctrl
from utils import Singleton
from configuration import AWSConfig
from model import DataTable
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log

class DataTableService(metaclass=Singleton):

    def __init__(self, aws_config:AWSConfig) -> None:
        """
        Initializes the DataTableService with AWS configuration and DynamoDB client.

        Args:
            aws_config (AWSConfig): The configuration object for AWS settings.
        """
        self.aws_config = aws_config
        self.dynamo_db = self.__configure_dynamodb()


    def list_tables(self, owner_id:str) -> list[DataTable]:
        """
        Retrieves the list of DynamoDB tables that belong to the specified owner.

        Args:
            owner_id (str): The owner ID to filter tables by their prefix.

        Returns:
            list[DataTable]: A list of DataTable objects containing table details.

        Raises:
            ServiceException: If AWS credentials are missing or incomplete, or any other error occurs.
        """
        log.info('Retrieving all tables. ownerId: %s', owner_id)
        try:
            # Fetch the list of tables
            response = self.dynamo_db.list_tables()
            table_names = response.get('TableNames', [])
            
            table_details = []
            for table_name in table_names:
                # Filter tables by owner_id prefix
                if table_name.startswith(owner_id):
                    # Fetch the details of table
                    table_detail = self.dynamo_db.describe_table(TableName=table_name)
                    table_info = DataTable(name=table_name,
                                           id=table_detail['Table']['TableId'],
                                           # Convert bytes to kilobytes
                                           size=table_detail['Table']['TableSizeBytes'] // 1024)
                    table_details.append(table_info)
            return table_details
        except NoCredentialsError:
            log.exception('Failed to retrieve tables. ownerId: %s', owner_id)
            raise ServiceException(403, ServiceStatus.FAILURE, 'AWS credentials not found')
        except PartialCredentialsError:
            log.exception('Failed to retrieve tables. ownerId: %s', owner_id)
            raise ServiceException(403, ServiceStatus.FAILURE, 'Incomplete AWS credentials')
        except Exception as e:
            log.exception('Failed to retrieve tables. ownerId: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, str(e))


    def __configure_dynamodb(self):
        """
        Configures the DynamoDB client based on the AWS configuration.

        Returns:
            boto3.client: The configured DynamoDB client.
        """
        dynamo_db = None

        if self.aws_config.is_local:
            # Use local DynamoDB instance
            dynamo_db = boto3.client('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url='http://localhost:8000')
        else:
            # Use AWS DynamoDB instance
            config = Config(region_name=self.aws_config.dynamodb_aws_region)
            dynamo_db = boto3.client('dynamodb', config=config)

        return dynamo_db

import boto3

from botocore.exceptions import ClientError
from typing import Union

from controller import common_controller as common_ctrl
from utils import Singleton
from configuration import S3AsssetsFileConfig
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log

class S3AssetsService(metaclass=Singleton):


    def __init__(self, s3_assets_file_config: S3AsssetsFileConfig) -> None:
        self.s3_client = boto3.client('s3')
        self.s3_assets_file_config = s3_assets_file_config


    def _generate_s3_key(self, owner_id:str, relative_path:str) -> str:
        """
        Generates an s3 key for the provided details.
        Args:
            owner_id (str): The owner for which the data should be gathered
            relative_path (str): The relative for the owner from its home directory including the filename
        Returns:
            str: S3 Key
        """
        relative_path = relative_path.lstrip('/')
        return f'{owner_id}/{relative_path}'


    def upload_script_to_s3(self, owner_id: str, relative_path: str, data: str) -> str:
        """
        Uploads the script content to S3 and returns the version ID.
        Raises a ServiceException if the upload fails.
        """
        log.info("Uploading script to s3. owner_id: %s, path: %s", owner_id, relative_path)
        try:
            key = self._generate_s3_key(owner_id, relative_path)
            response = self.s3_client.put_object(
                Bucket=self.s3_assets_file_config.assets_bucket_name,
                Key=key,
                Body=data
            )
            return response.get('VersionId')
        except ClientError as e:
            log.exception('Failed to upload object to s3. owner_id: %s, key: %s', owner_id, key)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to upload object to s3')
        

    def get_script_from_s3(self, owner_id: str, relative_path: str, version_id: Union[str, None] = None) -> str:
        """
        Retrieves the script content from S3 using the given version ID.
        Raises a ServiceException if the retrieval fails.
        """
        log.info("Fetching script from s3. owner_id: %s, path: %s", owner_id, relative_path)
        try:
            key = self._generate_s3_key(owner_id, relative_path)
            params = {
                'Bucket': self.s3_assets_file_config.assets_bucket_name, 
                'Key': key
            }
            if version_id:
                params['VersionId'] = version_id

            response = self.s3_client.get_object(**params)
            return response['Body'].read().decode('utf-8')
        except ClientError as e:
            log.exception('Failed to upload object to s3. owner_id: %s, key: %s', owner_id, key)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to fetch object from s3')
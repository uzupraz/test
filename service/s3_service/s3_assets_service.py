import boto3

from botocore.exceptions import ClientError
from typing import Union

from controller import common_controller as common_ctrl
from utils import Singleton
from configuration import S3AssetsFileConfig
from exception import ServiceException
from enums import ServiceStatus

log = common_ctrl.log

class S3AssetsService(metaclass=Singleton):


    def __init__(self, s3_assets_file_config: S3AssetsFileConfig) -> None:
        self.s3_client = boto3.client('s3')
        self.s3_assets_file_config = s3_assets_file_config

    def _generate_pre_signed_url(self, bucket_id:str, s3_key:str, action:str) -> str:
        """
        Generates a S3 presigned URL based on the provided information.
        Args:
            bucket_id(str): The bucket for which the presigned URL is to be generated
            s3_key(str): The S3 object key
            action(str): The action the url should allow. examples: put_object, get_object
        """
        log.info('Getting pre-signed url. bucket_id: %s, s3_key: %s, action: %s', bucket_id, s3_key, action)
        try:
            url = self.s3_client.generate_presigned_url(
                action,
                Params={
                    'Bucket': bucket_id,
                    'Key': s3_key
                },
                ExpiresIn=self.s3_assets_file_config.pre_signed_url_expiration
            )
            return url
        except ClientError as e:
            log.exception('Failed to get pre-signed url. bucket_id: %s, s3_key: %s, action: %s', bucket_id, s3_key, action)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Could not get pre-signed url')

    
    def generate_download_pre_signed_url(self ,s3_key:str) -> str:
        """
        Gets a pre-signed url to download a file from S3 Asset Bucket.
        Args:
            s3_key (str): The S3 object key
        Returns:
            str: Pre-signed URL
        """
        return self._generate_pre_signed_url(self.s3_assets_file_config.assets_bucket_name, s3_key, 'get_object')
    
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

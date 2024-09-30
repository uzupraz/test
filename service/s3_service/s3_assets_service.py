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

    
    def get_updates_url_from_s3(self ,s3_key:str) -> str:
        """
        Returns the updates url for modules in S3.
        """
        return self._generate_pre_signed_url(self.s3_assets_file_config.assets_bucket_name, s3_key, 'get_object')

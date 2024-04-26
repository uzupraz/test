import boto3

from botocore.exceptions import ClientError

from controller import common_controller as common_ctrl
from utils import Singleton
from configuration import AsyncFileDeliveryS3Config
from exception import ServiceException
from enums import ServiceStatus


log = common_ctrl.log


class S3FileService(metaclass=Singleton):


    def __init__(self, file_delivery_config: AsyncFileDeliveryS3Config) -> None:
        self.s3_client = boto3.client('s3')
        self.file_delivery_config = file_delivery_config


    def get_pre_signed_url(self, owner_id: str, file_name: str) -> str:
        """
        Gets a pre-signed url to upload a file to S3.

        Returns:
            str: Pre-signed URL
        """
        log.info('Getting pre-signed url. owner_id: %s, file_name: %s', owner_id, file_name)
        try:
            url = self.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.file_delivery_config.input_bucket_name,
                    'Key': f'{self.file_delivery_config.object_prefix}/{owner_id}/{file_name}'
                },
                ExpiresIn=self.file_delivery_config.pre_signed_url_expiration
            )
            return url
        except ClientError as e:
            log.exception('Failed to get pre-signed url. owner_id: %s, file_name: %s', owner_id, file_name)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Could not get pre-signed url')

import boto3

from botocore.exceptions import ClientError
from typing import List

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
        print(self.file_delivery_config)

    
    def _generate_s3_key(self, owner_id:str, relative_path:str) -> str:
        """
        Generates an s3 key for the provided details.
        Args:
            owner_id (str): The owner for which the data should be gathered
            relative_path (str): The relative for the owner from its home directory including the filename
        Returns:
            str: S3 Key
        """
        s3_key = ''
        relative_path = relative_path.lstrip('/')
        if self.file_delivery_config.object_prefix:
            s3_key = f'{self.file_delivery_config.object_prefix}/{owner_id}/{relative_path}'
        else:
            s3_key = f'{owner_id}/{relative_path}'
        return s3_key
    

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
                ExpiresIn=self.file_delivery_config.pre_signed_url_expiration
            )
            return url
        except ClientError as e:
            log.exception('Failed to get pre-signed url. bucket_id: %s, s3_key: %s, action: %s', bucket_id, s3_key, action)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Could not get pre-signed url')
    

    def list_files_in_output_folder(self, owner_id:str, relative_path:str) -> List[any]:
        """
        Lists files available in the output bucket. The folder is defined in the relative path from the home folder of the owner.
        Args:
            owner_id (str): The id of the owner for which the files is to be listed
            relative_path (str): The relative path of the folder from the home directory where the files are to be listed
        Returns:
            List[dict]: A list of dictionary with path and url
        """
        s3_key = self._generate_s3_key(owner_id, relative_path)
        response = self.s3_client.list_objects_v2(
            Bucket=self.file_delivery_config.output_bucket_name,
            Prefix=s3_key
        )
        results = []

        for obj in response.get('Contents', []):
            results.append({
                'path': obj['Key'],
                'url': self._generate_pre_signed_url(self.file_delivery_config.output_bucket_name, obj['Key'], 'get_object')
            })

        return results
    

    def move_file(self, source_bucket:str, source_key:str, dest_bucket:str, dest_key:str):
        """
        Moves a file from the defined source bucket to the defined destination bucket. Note that the buckets could also be the same.
        Args:
            source_bucket (str): The source bucket id
            source_key (str): Source Object key
            dest_bucket (str): Destination bucket id. Either different or the same
            dest_key (str): Destination object key
        """
        log.info('Moving S3 file. src_bucket: %s, src_key: %s, dest_bucket: %s, dest_key: %s', source_bucket, source_key, dest_bucket, dest_key)
        assert not (source_bucket == dest_bucket and source_key == dest_key ), 'Cannot move from same location to the same location (bucket + key)'
        try:
            self.s3_client.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={
                'Bucket': source_bucket,
                'Key': source_key
            })
        except ClientError as e:
            log.info('Failed to copy S3 file. src_bucket: %s, src_key: %s, dest_bucket: %s, dest_key: %s', source_bucket, source_key, dest_bucket, dest_key)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Could not move S3 File')
        log.info('Successfully copied file to destination bucket. src_bucket: %s, src_key: %s, dest_bucket: %s, dest_key: %s', source_bucket, source_key, dest_bucket, dest_key)

        try:
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        except ClientError as e:
            log.info('Failed to delete S3 file after copy. src_bucket: %s, src_key: %s, dest_bucket: %s, dest_key: %s', source_bucket, source_key, dest_bucket, dest_key)
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Could not move S3 File')
        log.info('Successfully deleted source file. src_bucket: %s, src_key: %s, dest_bucket: %s, dest_key: %s', source_bucket, source_key, dest_bucket, dest_key)


    def archive_output_file(self, owner_id:str, relative_path:str):
        """
        Archives the defined output file.
        Args:
            owner_id (str): The Owner Id who owns the file
            relative_path (str): The relative path to the file from the owner's home foldwer
        """
        log.info('Archiving output file. owner_id: %s, relative_path: %s', owner_id, relative_path)
        s3_key = self._generate_s3_key(owner_id, relative_path)
        self.move_file(self.file_delivery_config.output_bucket_name, s3_key, self.file_delivery_config.archive_bucket_name, s3_key)
        log.info('Successfully archived output file. owner_id: %s, relative_path: %s', owner_id, relative_path)


    def generate_upload_pre_signed_url(self, owner_id:str, relative_path:str) -> str:
        """
        Gets a pre-signed url to upload a file to S3 Input Bucket.
        Args:
            owner_id (str): The owner for which the data should be gathered
            relative_path (str): The relative for the owner from its home directory including the filename
        Returns:
            str: Pre-signed URL
        """
        s3_key = self._generate_s3_key(owner_id, relative_path)
        log.info('Getting pre-signed url. owner_id: %s, relative_path: %s', owner_id, relative_path)
        return self._generate_pre_signed_url(self.file_delivery_config.input_bucket_name, s3_key, 'put_object')
    

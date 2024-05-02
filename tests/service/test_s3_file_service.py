import unittest
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

from service import S3FileService
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus


class TestS3FileService(unittest.TestCase):


    @patch('service.s3_file_service.boto3.client')
    def setUp(self, mock_boto3_client) -> None:
        self.s3_client = MagicMock()
        mock_boto3_client.return_value = self.s3_client
        Singleton.clear_instance(S3FileService)
        self.file_delivery_config = MagicMock(input_bucket_name='input-bucket', output_bucket_name='output-bucket', archive_bucket_name='archive-bucket', pre_signed_url_expiration=3600)
        self.s3_file_service = S3FileService(self.file_delivery_config)


    def tearDown(self) -> None:
        del self.s3_file_service
        del self.file_delivery_config
        Singleton.clear_instance(S3FileService)


    def test_generate_s3_key_with_object_prefix(self):
        owner_id = "user123"
        relative_path = "documents/file.txt"
        expected_output = "prefix/user123/documents/file.txt"

        self.s3_file_service.file_delivery_config.object_prefix = "prefix"
        result = self.s3_file_service._generate_s3_key(owner_id, relative_path)
        self.assertEqual(result, expected_output)


    def test_generate_s3_key_without_object_prefix(self):
        owner_id = "user123"
        relative_path = "documents/file.txt"
        expected_output = "user123/documents/file.txt"

        self.s3_file_service.file_delivery_config.object_prefix = ""
        result = self.s3_file_service._generate_s3_key(owner_id, relative_path)
        self.assertEqual(result, expected_output)


    def test_generate_s3_key_with_leading_slash(self):
        owner_id = "user123"
        relative_path = "/documents/file.txt"
        expected_output = "user123/documents/file.txt"

        self.s3_file_service.file_delivery_config.object_prefix = ""
        result = self.s3_file_service._generate_s3_key(owner_id, relative_path)
        self.assertEqual(result, expected_output)


    def test_generate_s3_key_without_leading_slash(self):
        owner_id = "user123"
        relative_path = "documents/file.txt"
        expected_output = "user123/documents/file.txt"

        self.s3_file_service.file_delivery_config.object_prefix = ""
        result = self.s3_file_service._generate_s3_key(owner_id, relative_path)
        self.assertEqual(result, expected_output)


    def test_generate_pre_signed_url_success(self):
        # Mocking the s3_client to return a dummy URL
        self.s3_client.generate_presigned_url.return_value = 'http://dummy-url'
        url = self.s3_file_service._generate_pre_signed_url('bucket-id', 's3-key', 'get_object')
        self.assertEqual(url, 'http://dummy-url')
        self.s3_client.generate_presigned_url.assert_called_once_with('get_object', Params={'Bucket': 'bucket-id', 'Key': 's3-key'}, ExpiresIn=3600)


    def test_generate_pre_signed_url_should_raise_service_exception_in_case_of_aws_client_error(self):
        # Mocking the s3_client to raise a ClientError for invalid operation
        self.s3_file_service.s3_client.generate_presigned_url.side_effect = ClientError({'ResponseMetadata': {'HTTPStatusCode': 400}}, 'generate_presigned_url')
        with self.assertRaises(ServiceException):
            self.s3_file_service._generate_pre_signed_url('bucket-id', 's3-key', 'invalid_action')

        self.s3_client.generate_presigned_url.assert_called_once_with('invalid_action', Params={'Bucket': 'bucket-id', 'Key': 's3-key'}, ExpiresIn=3600)


    def test_list_files_in_output_folder_with_no_objects_should_return_empty_list(self):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        self.s3_client.list_objects_v2.return_value = {'Contents': []}
        result = self.s3_file_service.list_files_in_output_folder('owner123', 'path/to/folder')
        self.assertEqual(result, [])
        self.s3_client.list_objects_v2.assert_called_once_with(Bucket='output-bucket', Prefix='owner123/path/to/folder')


    def test_list_files_in_output_folder_objects_with_slash_at_the_end_should_return_empty_list(self):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        self.s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'path/'}]}
        result = self.s3_file_service.list_files_in_output_folder('owner123', 'path/to/folder')
        self.assertEqual(result, [])
        self.s3_client.list_objects_v2.assert_called_once_with(Bucket='output-bucket', Prefix='owner123/path/to/folder')


    @patch('service.s3_file_service.S3FileService._generate_pre_signed_url')
    def test_list_files_in_output_folder_objects_without_slash_at_the_key_path_with_single_file(self, mock_generate_pre_signed_url):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        self.s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'file.txt'}]}
        mock_generate_pre_signed_url.return_value = 'https://example.com/file.txt'
        result = self.s3_file_service.list_files_in_output_folder('owner123', 'path/to/folder')
        self.assertEqual(result, [{'path': 'file.txt', 'url': 'https://example.com/file.txt'}])
        self.s3_client.list_objects_v2.assert_called_once_with(Bucket='output-bucket', Prefix='owner123/path/to/folder')
        mock_generate_pre_signed_url.assert_called_once_with('output-bucket', 'file.txt', 'get_object')


    @patch('service.s3_file_service.S3FileService._generate_pre_signed_url')
    def test_list_files_in_output_folder_objects_without_slash_at_the_key_path_with_multiple_file(self, mock_generate_pre_signed_url):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        self.s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'file.txt'}, {'Key': 'file2.txt'}]}
        mock_generate_pre_signed_url.side_effect = ['https://example.com/file.txt', 'https://example.com/file2.txt']
        result = self.s3_file_service.list_files_in_output_folder('owner123', 'path/to/folder')
        self.assertEqual(result, [{'path': 'file.txt', 'url': 'https://example.com/file.txt'}, {'path': 'file2.txt', 'url': 'https://example.com/file2.txt'}])
        self.s3_client.list_objects_v2.assert_called_once_with(Bucket='output-bucket', Prefix='owner123/path/to/folder')
        mock_generate_pre_signed_url.assert_has_calls([call('output-bucket', 'file.txt', 'get_object'), call('output-bucket', 'file2.txt', 'get_object')])


    @patch('service.s3_file_service.S3FileService._generate_pre_signed_url')
    def test_list_files_in_output_folder_objects_should_return_path_in_response_without_owner_id_if_present(self, mock_generate_pre_signed_url):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        self.s3_client.list_objects_v2.return_value = {'Contents': [{'Key': 'owner123/file.txt'}]}
        mock_generate_pre_signed_url.return_value = 'https://example.com/file.txt'
        result = self.s3_file_service.list_files_in_output_folder('owner123', 'path/to/folder')
        self.assertEqual(result, [{'path': '/file.txt', 'url': 'https://example.com/file.txt'}])
        self.s3_client.list_objects_v2.assert_called_once_with(Bucket='output-bucket', Prefix='owner123/path/to/folder')
        mock_generate_pre_signed_url.assert_called_once_with('output-bucket', 'owner123/file.txt', 'get_object')


    def test_move_file_with_different_destination_bucket_and_with_same_object_key_should_move_file_successfully(self):
        self.s3_client.copy_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.s3_client.delete_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.s3_file_service.move_file('input-bucket', 'key.txt', 'output-bucket', 'key.txt')
        self.s3_client.copy_object.assert_called_once_with(Bucket='output-bucket', CopySource={'Bucket': 'input-bucket', 'Key': 'key.txt'}, Key='key.txt')
        self.s3_client.delete_object.assert_called_once_with(Bucket='input-bucket', Key='key.txt')


    def test_move_file_with_same_destination_bucket_and_with_same_object_key_should_not_move_file_and_raise_error(self):
        with self.assertRaises(AssertionError):
            self.s3_file_service.move_file('input-bucket', 'key.txt', 'input-bucket', 'key.txt')
        self.s3_client.copy_object.assert_not_called()
        self.s3_client.delete_object.assert_not_called()


    def test_move_file_with_error_while_copying_should_raise_service_exception_and_should_not_attempt_deleting_of_object(self):
        self.s3_client.copy_object.side_effect = ClientError({'ResponseMetadata': {'HTTPStatusCode': 400}}, 'copy_object')
        with self.assertRaises(ServiceException):
            self.s3_file_service.move_file('input-bucket', 'key.txt', 'output-bucket', 'key.txt')
        self.s3_client.copy_object.assert_called_once_with(Bucket='output-bucket', CopySource={'Bucket': 'input-bucket', 'Key': 'key.txt'}, Key='key.txt')
        self.s3_client.delete_object.assert_not_called()


    def test_move_file_with_successful_copying_but_error_while_deleting_object_should_raise_service_exception(self):
        self.s3_client.delete_object.side_effect = ClientError({'ResponseMetadata': {'HTTPStatusCode': 400}}, 'delete_object')
        with self.assertRaises(ServiceException) as ex:
            self.s3_file_service.move_file('input-bucket', 'key.txt', 'output-bucket', 'key.txt')
        self.assertEqual(ex.exception.status_code, 400)
        self.assertEqual(ex.exception.status, ServiceStatus.FAILURE)
        self.s3_client.copy_object.assert_called_once_with(Bucket='output-bucket', CopySource={'Bucket': 'input-bucket', 'Key': 'key.txt'}, Key='key.txt')
        self.s3_client.delete_object.assert_called_once_with(Bucket='input-bucket', Key='key.txt')


    @patch('service.s3_file_service.S3FileService.move_file')
    def test_archive_output_file_success(self, mock_move_file):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        self.s3_file_service.archive_output_file('owner_id', 'path/to/file.txt')
        mock_move_file.assert_called_once_with('output-bucket', 'owner_id/path/to/file.txt', 'archive-bucket', 'owner_id/path/to/file.txt')


    @patch('service.s3_file_service.S3FileService.move_file')
    def test_archive_output_file_should_raise_service_exception_when_failed_to_move_file(self, mock_move_file):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        mock_move_file.side_effect = ServiceException(400, ServiceStatus.FAILURE, 'Something went wrong')
        with self.assertRaises(ServiceException):
            self.s3_file_service.archive_output_file('owner_id', 'path/to/file.txt')
        mock_move_file.assert_called_once_with('output-bucket', 'owner_id/path/to/file.txt', 'archive-bucket', 'owner_id/path/to/file.txt')


    @patch('service.s3_file_service.S3FileService._generate_pre_signed_url')
    def test_generate_upload_pre_signed_url_success(self, mock_generate_pre_signed_url):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        mock_generate_pre_signed_url.return_value = 'https://example.com/file.txt'
        result = self.s3_file_service.generate_upload_pre_signed_url('owner_id', 'path/to/file.txt')
        self.assertEqual(result, 'https://example.com/file.txt')
        mock_generate_pre_signed_url.assert_called_once_with('input-bucket', 'owner_id/path/to/file.txt', 'put_object')


    @patch('service.s3_file_service.S3FileService._generate_pre_signed_url')
    def test_generate_upload_pre_signed_url_should_raise_service_exception_when_failed_to_generate(self, mock_generate_pre_signed_url):
        self.s3_file_service.file_delivery_config.object_prefix = ""
        mock_generate_pre_signed_url.side_effect = ServiceException(400, ServiceStatus.FAILURE, 'Something went wrong')
        with self.assertRaises(ServiceException):
            result = self.s3_file_service.generate_upload_pre_signed_url('owner_id', 'path/to/file.txt')

        mock_generate_pre_signed_url.assert_called_once_with('input-bucket', 'owner_id/path/to/file.txt', 'put_object')
import unittest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from repository import DataFormatRepository
from tests import TestUtils
from enums import ServiceStatus
from exception import ServiceException
from utils import Singleton


class TestDataFormatRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/data_format/'


    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(DataFormatRepository)
        with patch('repository.data_format_repository.DataFormatRepository._DataFormatRepository__configure_dynamodb') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.data_format_repository = DataFormatRepository(self.app_config, self.aws_config)


    def tearDown(self) -> None:
        self.app_config = None
        self.aws_config = None
        self.data_format_repository = None


    def test_get_data_formats_success(self):
        mock_response_path = self.test_resource_path + '/get_data_formats_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        
        self.data_format_repository.table.scan = MagicMock(return_value={"Items": mock_response_items})

        actual_result = self.data_format_repository.get_data_formats()

        self.assertEqual(len(mock_response_items), len(actual_result))
        self.data_format_repository.table.scan.assert_called_once()

    
    def test_get_data_formats_with_empty_list(self):
        self.data_format_repository.table.scan = MagicMock(return_value={"Items": []})

        actual_result = self.data_format_repository.get_data_formats()

        self.assertEqual(len(actual_result), 0)
        self.data_format_repository.table.scan.assert_called_once()


    def test_get_data_formats_should_throw_client_exception(self):
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.data_format_repository.table.scan = MagicMock()
        self.data_format_repository.table.scan.side_effect = ClientError(error_response, 'scan')

        with self.assertRaises(ServiceException) as context:
            self.data_format_repository.get_data_formats()

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Error while retrieving data formats')
        self.data_format_repository.table.scan.assert_called_once()
    
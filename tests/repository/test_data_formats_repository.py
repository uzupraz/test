import unittest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from dacite import from_dict
from repository import DataFormatsRepository
from tests import TestUtils
from enums import ServiceStatus
from exception import ServiceException
from utils import Singleton
from model import DataFormat


class TestDataFormatsRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/data_format/'


    def setUp(self) -> None:
        """Set up mock DynamoDB table and repository instance before each test."""
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(DataFormatsRepository)
        with patch('repository.data_formats_repository.DataFormatsRepository._DataFormatsRepository__configure_dynamodb') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.data_formats_repository = DataFormatsRepository(self.app_config, self.aws_config)


    def tearDown(self) -> None:
        """Clean up configurations and repository instance after each test."""
        self.app_config = None
        self.aws_config = None
        self.data_formats_repository = None


    def test_list_all_data_formats_success(self):
        """Test that data formats are retrieved successfully with the expected item count."""
        mock_response_path = self.test_resource_path + '/list_all_data_formats_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        
        self.data_formats_repository.table.scan = MagicMock(return_value={"Items": mock_response_items})

        actual_result = self.data_formats_repository.list_all_data_formats()

        self.assertEqual(len(mock_response_items), len(actual_result))
        self.data_formats_repository.table.scan.assert_called_once()

    
    def test_list_all_data_formats_with_empty_list(self):
        """Test that an empty list is returned when there are no data formats in the table."""
        self.data_formats_repository.table.scan = MagicMock(return_value={"Items": []})

        actual_result = self.data_formats_repository.list_all_data_formats()

        self.assertEqual(len(actual_result), 0)
        self.data_formats_repository.table.scan.assert_called_once()


    def test_list_all_data_formats_should_throw_client_exception(self):
        """Test that a ServiceException is raised when a ClientError occurs during data format retrieval."""
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.data_formats_repository.table.scan = MagicMock()
        self.data_formats_repository.table.scan.side_effect = ClientError(error_response, 'scan')

        with self.assertRaises(ServiceException) as context:
            self.data_formats_repository.list_all_data_formats()

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Error while retrieving data formats')
        self.data_formats_repository.table.scan.assert_called_once()


    def test_get_data_format_success(self):
        """Test that data format is retrieved successfully."""
        format_name="CSV"
        mock_response_path = self.test_resource_path + 'get_data_format_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        
        self.data_formats_repository.table.query = MagicMock(return_value={"Items": [mock_response_items]})

        actual_result = self.data_formats_repository.get_data_format(format_name)

        self.assertEqual(from_dict(DataFormat, mock_response_items), actual_result)
        self.assertEqual(actual_result.format_name, format_name)
        self.data_formats_repository.table.query.assert_called_once_with(
            KeyConditionExpression=Key('format_name').eq(format_name)
        )

    
    def test_get_data_format_with_none_for_non_existing_data(self):
        """Test that None is returned when there are no data format in the table."""
        format_name="CSV"
        self.data_formats_repository.table.query = MagicMock(return_value={"Items": []})

        actual_result = self.data_formats_repository.get_data_format(format_name)

        self.assertIsNone(actual_result)
        self.data_formats_repository.table.query.assert_called_once_with(
            KeyConditionExpression=Key('format_name').eq(format_name)
        )


    def test_get_data_format_should_throw_client_exception(self):
        """Test that a ServiceException is raised when a ClientError occurs during data format retrieval."""
        format_name="CSV"
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.data_formats_repository.table.query = MagicMock()
        self.data_formats_repository.table.query.side_effect = ClientError(error_response, 'query')

        with self.assertRaises(ServiceException) as context:
            self.data_formats_repository.get_data_format(format_name)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Error while retrieving data format')
        self.data_formats_repository.table.query.assert_called_once_with(
            KeyConditionExpression=Key('format_name').eq(format_name)
        )
    
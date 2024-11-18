import unittest
from unittest.mock import MagicMock
from dacite import from_dict

from enums import ServiceStatus
from exception import ServiceException
from model import DataFormat
from service import DataFormatsService
from tests import TestUtils


class TestDataFormatsService(unittest.TestCase):

    test_resource_path = '/tests/resources/data_format/'


    def setUp(self) -> None:
        """Set up mock repository and service instances before each test."""
        self.data_formats_repository = MagicMock()
        self.data_formats_service = DataFormatsService(self.data_formats_repository)


    def tearDown(self) -> None:
        """Clean up service and repository instances after each test."""
        self.data_formats_repository = None
        self.data_formats_service = None


    def test_list_all_data_formats_success(self):
        """Test that data formats are retrieved successfully from the service with the expected items."""
        mock_response_path = self.test_resource_path + '/list_all_data_formats_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        mock_data_formats = [
            from_dict(DataFormat, item)
            for item in mock_response_items
        ]
        self.data_formats_service.data_formats_repository.list_all_data_formats = MagicMock(return_value=mock_data_formats)

        actual_result = self.data_formats_service.list_all_data_formats()

        self.assertListEqual(mock_data_formats, actual_result)
        self.data_formats_service.data_formats_repository.list_all_data_formats.assert_called_once()


    def test_list_all_data_formats_should_return_empty_list(self):
        """Test that an empty list is returned by the service when there are no data formats in the repository."""
        mock_response_path = self.test_resource_path + '/list_all_data_formats_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        mock_data_formats = [
            from_dict(DataFormat, item)
            for item in mock_response_items
        ]
        self.data_formats_service.data_formats_repository.list_all_data_formats = MagicMock(return_value=mock_data_formats)

        actual_result = self.data_formats_service.list_all_data_formats()

        self.assertListEqual(mock_data_formats, actual_result)
        self.data_formats_service.data_formats_repository.list_all_data_formats.assert_called_once()


    def test_list_all_data_formats_should_throw_service_exception(self):
        """Test that a ServiceException is raised when there is an error retrieving data formats."""
        self.data_formats_service.data_formats_repository.list_all_data_formats = MagicMock()
        self.data_formats_service.data_formats_repository.list_all_data_formats.side_effect = ServiceException(500, ServiceStatus.FAILURE, 'Error while retrieving data formats')

        with self.assertRaises(ServiceException):
            self.data_formats_service.list_all_data_formats()
        
        self.data_formats_service.data_formats_repository.list_all_data_formats.assert_called_once()


    def test_get_data_format_success(self):
        """Test that data format is retrieved successfully from the service."""
        format_name = "dummy_name"
        mock_response_path = self.test_resource_path + '/get_data_format_response.json'
        mock_response_item = TestUtils.get_file_content(mock_response_path)

        mock_data_format = from_dict(DataFormat, mock_response_item)
        self.data_formats_service.data_formats_repository.get_data_format = MagicMock(return_value = mock_data_format)

        actual_result = self.data_formats_service.get_data_format(format_name)
        self.assertEqual(mock_data_format, actual_result)
        self.data_formats_service.data_formats_repository.get_data_format.assert_called_once_with(format_name)


    def test_get_data_format_should_return_none(self):
        """Test that None is returned by the service when there are no data format in the repository."""
        format_name="dummy_name"
        self.data_formats_service.data_formats_repository.get_data_format = MagicMock(return_value=None)

        actual_result = self.data_formats_service.get_data_format(format_name)

        self.assertIsNone(actual_result)
        self.data_formats_service.data_formats_repository.get_data_format.assert_called_once_with(format_name)


    def test_get_data_format_should_throw_service_exception(self):
        """Test that a ServiceException is raised when there is an error retrieving data format."""
        format_name = "dummy_name"
        self.data_formats_service.data_formats_repository.get_data_format = MagicMock()
        self.data_formats_service.data_formats_repository.get_data_format.side_effect = ServiceException(500, ServiceStatus.FAILURE, 'Error while retrieving data formats')
        
        with self.assertRaises(ServiceException):
            self.data_formats_service.get_data_format(format_name)
        
        self.data_formats_service.data_formats_repository.get_data_format.assert_called_once_with(format_name)

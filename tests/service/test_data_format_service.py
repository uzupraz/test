import unittest
from unittest.mock import MagicMock
from dacite import from_dict

from enums import ServiceStatus
from exception import ServiceException
from model import DataFormat
from service import DataFormatService
from tests import TestUtils


class TestDataFormatService(unittest.TestCase):

    test_resource_path = '/tests/resources/data_format/'


    def setUp(self) -> None:
        self.data_format_repository = MagicMock()
        self.data_format_service = DataFormatService(self.data_format_repository)


    def tearDown(self) -> None:
        self.data_format_service = None
        self.data_format_repository = None


    def test_get_data_formats_success(self):
        mock_response_path = self.test_resource_path + '/get_data_formats_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        mock_data_formats = [
            from_dict(DataFormat, item)
            for item in mock_response_items
        ]
        self.data_format_service.data_format_repository.get_data_formats = MagicMock(return_value=mock_data_formats)

        actual_result = self.data_format_service.get_data_formats()

        self.assertListEqual(mock_data_formats, actual_result)
        self.data_format_service.data_format_repository.get_data_formats.assert_called_once()


    def test_get_data_formats_should_return_empty_list(self):
        mock_response_path = self.test_resource_path + '/get_data_formats_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        mock_data_formats = [
            from_dict(DataFormat, item)
            for item in mock_response_items
        ]
        self.data_format_service.data_format_repository.get_data_formats = MagicMock(return_value=mock_data_formats)

        actual_result = self.data_format_service.get_data_formats()

        self.assertListEqual(mock_data_formats, actual_result)
        self.data_format_service.data_format_repository.get_data_formats.assert_called_once()


    def test_get_data_formats_should_throw_service_exception(self):
        self.data_format_service.data_format_repository.get_data_formats = MagicMock()
        self.data_format_service.data_format_repository.get_data_formats.side_effect = ServiceException(500, ServiceStatus.FAILURE, 'Error while retrieving data formats')

        with self.assertRaises(ServiceException):
            self.data_format_service.get_data_formats()
        
        self.data_format_service.data_format_repository.get_data_formats.assert_called_once()

import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from unittest.mock import MagicMock

from repository.customer_table_repository import CustomerTableRepository
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton
from tests.test_utils import TestUtils


class TestCustomerTableRepository(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_table/'


    def setUp(self):
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_resource = Mock()
        self.mock_dynamodb_client = Mock()

        Singleton.clear_instance(CustomerTableRepository)
        with patch('repository.customer_table_repository.CustomerTableRepository._CustomerTableRepository__configure_dynamodb_resource') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            self.mock_configure_resource.return_value = self.mock_dynamodb_resource
            self.customer_table_repository = CustomerTableRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.customer_table_repository = None


    def test_get_table_content_success_case(self):
        """
        Test case for successfully retrieving table content.

        Expected Result: The method returns a list of items and a last evaluated key.
        """
        table_name = 'TestTable'
        limit = 10
        exclusive_start_key = None

        # Mock response from DynamoDB scan
        mock_table_content_items_path = self.TEST_RESOURCE_PATH + "get_table_content_items_happy_case.json"
        mock_items = TestUtils.get_file_content(mock_table_content_items_path)
        mock_last_evaluated_key = {"key": "value"}

        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.scan.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': mock_last_evaluated_key
        }

        # Call the method under test
        items, last_evaluated_key = self.customer_table_repository.get_table_content(table_name, limit, exclusive_start_key)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.scan.assert_called_once_with(Limit=limit)
        self.assertEqual(items, mock_items)
        self.assertEqual(last_evaluated_key, mock_last_evaluated_key)


    def test_get_table_content_throws_service_exception(self):
        """
        Test case for handling ClientError while retrieving table content.

        Expected Result: The method raises a ServiceException.
        """
        table_name = 'TestTable'
        limit = 10
        exclusive_start_key = None

        # Mock ClientError exception from DynamoDB scan
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.scan.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 500}}, 'scan'
        )

        # Call the method under test and assert exception
        with self.assertRaises(ServiceException) as context:
            self.customer_table_repository.get_table_content(table_name, limit, exclusive_start_key)

        # Assertions
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve table items')
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.scan.assert_called_once_with(Limit=limit)
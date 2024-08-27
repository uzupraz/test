import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from unittest.mock import MagicMock
from boto3.dynamodb.conditions import Key, Attr

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

        Singleton.clear_instance(CustomerTableRepository)
        with patch('repository.customer_table_repository.CustomerTableRepository._CustomerTableRepository__configure_dynamodb_resource') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            self.mock_configure_resource.return_value = self.mock_dynamodb_resource
            self.customer_table_repository = CustomerTableRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.customer_table_repository = None


    def test_get_table_items_success_case(self):
        """
        Test case for successfully retrieving table content from customers table.

        Expected Result: The method returns a list of items and a last evaluated key.
        """
        table_name = 'TestTable'
        limit = 10
        exclusive_start_key = None

        # Mock response from DynamoDB scan
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_table_items_happy_case.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)
        mock_last_evaluated_key = {"key": "value"}

        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.scan.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': mock_last_evaluated_key
        }

        # Call the method under test
        items, last_evaluated_key = self.customer_table_repository.get_table_items(table_name, limit, exclusive_start_key)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.scan.assert_called_once_with(Limit=limit)
        self.assertEqual(items, mock_items)
        self.assertEqual(last_evaluated_key, mock_last_evaluated_key)


    def test_get_table_items_with_using_last_evaluated_key(self):
        """
        Test case for successfully retrieving table content from customers table with last evaluated key and limit.

        Expected Result: The method returns a list of items and a last evaluated key.
        """
        table_name = 'TestTable'
        limit = 10
        exclusive_start_key = {"last_key":"last_value"}

        # Mock response from DynamoDB scan
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_table_items_happy_case.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)
        mock_last_evaluated_key = {"key": "value"}

        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.scan.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': mock_last_evaluated_key
        }

        # Call the method under test
        items, last_evaluated_key = self.customer_table_repository.get_table_items(table_name, limit, exclusive_start_key)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.scan.assert_called_once_with(Limit=limit,ExclusiveStartKey=exclusive_start_key)
        self.assertEqual(items, mock_items)
        self.assertEqual(last_evaluated_key, mock_last_evaluated_key)

    
    def test_get_table_items_without_using_last_evaluated_key(self):
        """
        Test case for successfully retrieving table items without using last evaluated key.

        Expected Result: The method returns a list of items and a last evaluated key.
        """
        table_name = 'TestTable'
        limit = 10
        exclusive_start_key = None

        # Mock response from DynamoDB scan
        mock_table_content_items_path = self.TEST_RESOURCE_PATH + "get_table_items_happy_case.json"
        mock_items = TestUtils.get_file_content(mock_table_content_items_path)

        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.scan.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        items, last_evaluated_key = self.customer_table_repository.get_table_items(table_name, limit, exclusive_start_key)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.scan.assert_called_once_with(Limit=limit)
        self.assertEqual(items, mock_items)


    def test_get_table_items_throws_service_exception(self):
        """
        Test case for handling ClientError while fetching customer table items.

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
            self.customer_table_repository.get_table_items(table_name, limit, exclusive_start_key)

        # Assertions
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve table items')
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.scan.assert_called_once_with(Limit=limit)

    
    def test_create_item_success_case(self):
        """
        Test case for successfully inserting an item into the DynamoDB table.
        """
        table_name = 'TestTable'
        item = {
            'id': '12345',
            'name': 'Sample Item',
            'attributes': {'color': 'blue', 'size': 'large'}
        }

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.put_item.return_value = {}  # Mocking successful put_item response

        # Call the method under test
        result = self.customer_table_repository.create_item(table_name, item)
        
        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.put_item.assert_called_once_with(Item=item)
        self.assertEqual(result, item)


    def test_create_item_throws_service_exception(self):
        """
        Test case for handling ClientError while creating an item into the DynamoDB table.
        """
        table_name = 'TestTable'
        item = {
            'id': '12345',
            'name': 'Sample Item',
            'attributes': {'color': 'blue', 'size': 'large'}
        }

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.put_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 500}}, 'put_item'
        )

        # Call the method under test and assert exception
        with self.assertRaises(ServiceException) as context:
            self.customer_table_repository.create_item(table_name, item)

        # Assertions
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to insert item into table')
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.put_item.assert_called_once_with(Item=item)


    def test_delete_item_success_case(self):
        """
        Test case for successfully deleting an item from the DynamoDB table.
        """
        table_name = 'TestTable'
        key = {'id': '12345'}

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.delete_item.return_value = {}  # Mocking successful delete_item response

        # Call the method under test
        self.customer_table_repository.delete_item(table_name, key)
        
        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.delete_item.assert_called_once_with(Key=key)


    def test_delete_item_throws_service_exception(self):
        """
        Test case for handling ClientError while deleting an item from the DynamoDB table.
        """
        table_name = 'TestTable'
        key = {'id': '12345'}

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.delete_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 500}}, 'delete_item'
        )

        # Call the method under test and assert exception
        with self.assertRaises(ServiceException) as context:
            self.customer_table_repository.delete_item(table_name, key)

        # Assertions
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to delete item from table')
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.delete_item.assert_called_once_with(Key=key)

    
    def test_query_item_with_partition_key_only(self):
        """
        Test querying an item using only the partition key.
        """
        table_name = 'TestTable'
        partition = ('id', '12345')
        sort = None

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.query.return_value = {'Items': [{'id': '12345'}]}  # Mock successful query response

        # Call the method under test
        result = self.customer_table_repository.query_item(table_name, partition, sort)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.query.assert_called_once_with(KeyConditionExpression=Key('id').eq('12345'))
        self.assertEqual(result, [{'id': '12345'}])


    def test_query_item_with_partition_and_sort_key(self):
        """
        Test querying an item using both partition and sort keys.
        """
        table_name = 'TestTable'
        partition = ('id', '12345')
        sort = ('created_at', '2023-01-01')

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.query.return_value = {'Items': [{'id': '12345', 'created_at': '2023-01-01'}]}  # Mock successful query response

        # Call the method under test
        result = self.customer_table_repository.query_item(table_name, partition, sort)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq('12345') & Key('created_at').eq('2023-01-01')
        )
        self.assertEqual(result, [{'id': '12345', 'created_at': '2023-01-01'}])


    def test_query_item_with_filters(self):
        """
        Test querying an item using partition, sort keys, and additional filters.
        """
        table_name = 'TestTable'
        partition = ('id', '12345')
        sort = ('created_at', '2023-01-01')
        filters = {'status': 'active'}

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.query.return_value = {'Items': [{'id': '12345', 'created_at': '2023-01-01', 'status': 'active'}]}  # Mock successful query response

        # Call the method under test
        result = self.customer_table_repository.query_item(table_name, partition, sort, filters)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq('12345') & Key('created_at').eq('2023-01-01'),
            FilterExpression=Attr('status').eq('active')
        )
        self.assertEqual(result, [{'id': '12345', 'created_at': '2023-01-01', 'status': 'active'}])


    def test_query_item_no_results(self):
        """
        Test querying an item that does not exist.
        """
        table_name = 'TestTable'
        partition = ('id', '99999')
        sort = None

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.query.return_value = {'Items': []}  # Mock empty query response

        # Call the method under test and assert exception
        data = self.customer_table_repository.query_item(table_name, partition, sort)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.query.assert_called_once_with(KeyConditionExpression=Key('id').eq('99999'))
        self.assertEqual(data, [])


    def test_query_item_throws_service_exception(self):
        """
        Test handling of ClientError while querying an item from the DynamoDB table.
        """
        table_name = 'TestTable'
        partition = ('id', '12345')
        sort = None

        # Mock DynamoDB table
        mock_table = MagicMock()
        self.mock_dynamodb_resource.Table.return_value = mock_table
        mock_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 500}}, 'query'
        )

        # Call the method under test and assert exception
        with self.assertRaises(ServiceException) as context:
            self.customer_table_repository.query_item(table_name, partition, sort)

        # Assertions
        self.mock_dynamodb_resource.Table.assert_called_once_with(table_name)
        mock_table.query.assert_called_once_with(KeyConditionExpression=Key('id').eq('12345'))
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to query item from table')

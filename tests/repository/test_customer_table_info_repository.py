import unittest
from unittest.mock import MagicMock, Mock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from repository.customer_table_info_repository import CustomerTableInfoRepository
from configuration import AWSConfig, AppConfig
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

class TestCustomerTableInfoRepository(unittest.TestCase):


    def setUp(self):
        app_config = Mock()
        app_config.customer_table_info_table_name = 'customer_table_info'
        aws_config = Mock()
        aws_config.dynamodb_aws_region = 'eu-central-1'

        Singleton.clear_instance(CustomerTableInfoRepository)
        self.customer_table_info_repo = CustomerTableInfoRepository(app_config, aws_config)


    def tearDown(self):
        self.customer_table_info_repo = None


    def test_get_tables_for_owner_happy_case(self):
        """
        Should return a list of tables for a valid owner_id.
        """
        owner_id = 'owner123'
        expected_items = [
            {'table_id': 'table123', 'table_name': 'Table1', 'original_table_name': 'OriginalTable1'},
            {'table_id': 'table456', 'table_name': 'Table2', 'original_table_name': 'OriginalTable2'}
        ]
        mock_table = MagicMock()
        self.customer_table_info_repo.table = mock_table
        mock_table.query.return_value = {'Items': expected_items}

        result = self.customer_table_info_repo.get_tables_for_owner(owner_id)

        mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(result, expected_items)


    def test_get_tables_for_owner_should_return_empty_tables(self):
        """
        Should return an empty list when there are no tables for the specified owner_id.
        """
        owner_id = 'owner123'
        mock_table = MagicMock()
        self.customer_table_info_repo.table = mock_table
        mock_table.query.return_value = {'Items': []}

        result = self.customer_table_info_repo.get_tables_for_owner(owner_id)

        mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(result, [])


    def test_get_tables_for_owner_with_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        owner_id = 'owner123'
        mock_table = MagicMock()
        self.customer_table_info_repo.table = mock_table
        mock_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_tables_for_owner(owner_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE.value)
        self.assertEqual(context.exception.message, 'Test Error')
        mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))


    def test_get_table_size_happy_case(self):
        """
        Should return the correct size of the table.
        """
        table_name = 'Table1'
        expected_size_kb = 1024
        mock_dynamodb_client = MagicMock()
        self.customer_table_info_repo.dynamodb_client = mock_dynamodb_client
        mock_dynamodb_client.describe_table.return_value = {
            'Table': {'TableSizeBytes': expected_size_kb * 1024}
        }
        self.customer_table_info_repo.dynamodb_client = mock_dynamodb_client

        result = self.customer_table_info_repo.get_table_size(table_name)

        mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)
        self.assertEqual(result, expected_size_kb)


    def test_get_table_size_empty_case(self):
        """
        Should return size 0 when the table has no items.
        """
        table_name = 'Table1'
        expected_size_kb = 0
        mock_dynamodb_client = MagicMock()
        self.customer_table_info_repo.dynamodb_client = mock_dynamodb_client
        mock_dynamodb_client.describe_table.return_value = {
            'Table': {'TableSizeBytes': expected_size_kb}
        }
        self.customer_table_info_repo.dynamodb_client = mock_dynamodb_client

        result = self.customer_table_info_repo.get_table_size(table_name)

        mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)
        self.assertEqual(result, expected_size_kb)


    def test_get_table_size_with_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        table_name = 'Table1'
        mock_dynamodb_client = MagicMock()
        self.customer_table_info_repo.dynamodb_client = mock_dynamodb_client
        mock_dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table')
        self.customer_table_info_repo.dynamodb_client = mock_dynamodb_client

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_size(table_name)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE.value)
        self.assertEqual(context.exception.message, 'Test Error')
        mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)

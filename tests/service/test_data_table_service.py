import unittest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from repository.customer_table_info_repository import CustomerTableInfoRepository
from service.data_table_service import DataTableService
from configuration import AWSConfig, AppConfig
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

class TestDataTableService(unittest.TestCase):


    def setUp(self):
        app_config = AppConfig(customer_table_info_table_name='customer_table_info')
        aws_config = AWSConfig(is_local=True, dynamodb_aws_region='eu-central-1')

        Singleton.clear_instance(CustomerTableInfoRepository)
        self.customer_table_info_repo = CustomerTableInfoRepository(app_config, aws_config)

        Singleton.clear_instance(DataTableService)
        self.data_table_service = DataTableService(self.customer_table_info_repo)


    def tearDown(self):
        self.app_config = None
        self.aws_config = None
        self.customer_table_info_repo = None
        self.data_table_service = None


    def test_list_tables_with_valid_owner_id(self):
        """
        Should return a list of DataTable objects for a valid owner_id.
        """
        owner_id = 'owner123'
        table_details = [
            {'original_table_name': 'originalTable1', 'table_name': 'Table1', 'table_id': 'table123'},
            {'original_table_name': 'originalTable2', 'table_name': 'Table2', 'table_id': 'table456'}
        ]

        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': table_details})
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            {'Table': {'TableSizeBytes': 1024 * 1024}},
            {'Table': {'TableSizeBytes': 2048 * 1024}}
        ])

        result = self.data_table_service.list_tables(owner_id)

        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='originalTable1')
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='originalTable2')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, 'Table1')
        self.assertEqual(result[0].id, 'table123')
        self.assertEqual(result[0].size, 1024)
        self.assertEqual(result[1].name, 'Table2')
        self.assertEqual(result[1].id, 'table456')
        self.assertEqual(result[1].size, 2048)


    def test_list_tables_with_empty_list_of_tables_for_owner(self):
        """
        Should return an empty list when there are no tables for the specified owner_id.
        """
        owner_id = 'owner123'
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': []})

        result = self.data_table_service.list_tables(owner_id)

        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(len(result), 0)


    def test_list_tables_for_owner_should_return_list_of_tables_with_empty_size(self):
        """
        Should handle tables with empty size (size 0).
        """
        owner_id = 'owner123'
        expected_items = [
            {'table_id': 'table123', 'table_name': 'Table1', 'original_table_name': 'OriginalTable1'},
            {'table_id': 'table456', 'table_name': 'Table2', 'original_table_name': 'OriginalTable2'}
        ]
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': expected_items})
        # Mock describe_table to return 0 size for one table and a specific size for another
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            {'Table': {'TableSizeBytes': 0}},
            {'Table': {'TableSizeBytes': 2048 * 1024}}
        ])

        result = self.data_table_service.list_tables(owner_id)

        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        # Verify describe_table calls
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='OriginalTable1')
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='OriginalTable2')

        # Verify the result contains tables with correct sizes
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, 'Table1')
        self.assertEqual(result[0].id, 'table123')
        # Check for empty size handling
        self.assertEqual(result[0].size, 0)
        self.assertEqual(result[1].name, 'Table2')
        self.assertEqual(result[1].id, 'table456')
        self.assertEqual(result[1].size, 2048)


    def test_list_tables_throws_service_exception_while_getting_list_of_tables_from_repository(self):
        """
        Should propagate ServiceException When there is any failure from repository.
        The failure could be as ClientError from dynamo DB or when the owner id has value as None etc.
        """
        owner_id = 'owner123'
        self.customer_table_info_repo.table.query = MagicMock(side_effect=ServiceException(500, ServiceStatus.FAILURE.value, 'Some error'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.list_tables(owner_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE.value)
        self.assertEqual(context.exception.message, 'Some error')
        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))


    def test_get_tables_for_owner_returns_list_of_tables_but_throws_service_exception(self):
        """
        Returns list of tables from repository sucessfully but propagates ServiceException
        while getting the size of the table.
        """
        owner_id = 'owner123'
        expected_items = [
            {'table_id': 'table123', 'table_name': 'Table1', 'original_table_name': 'OriginalTable1'},
            {'table_id': 'table456', 'table_name': 'Table2', 'original_table_name': 'OriginalTable2'}
        ]
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': expected_items})

        # Mock describe_table to throw a ClientError
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.list_tables(owner_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE.value)
        self.assertEqual(context.exception.message, 'Test Error')
        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')

import unittest
from unittest.mock import MagicMock, Mock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from tests.test_utils import TestUtils
from repository.customer_table_info_repository import CustomerTableInfoRepository
from service.data_table_service import DataTableService
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

class TestDataTableService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_table/'


    def setUp(self):
        app_config = Mock()
        app_config.customer_table_info_table_name = 'customer_table_info'
        aws_config = Mock()
        aws_config.dynamodb_aws_region = 'eu-central-1'

        Singleton.clear_instance(CustomerTableInfoRepository)
        self.customer_table_info_repo = CustomerTableInfoRepository(app_config, aws_config)

        Singleton.clear_instance(DataTableService)
        self.data_table_service = DataTableService(self.customer_table_info_repo)


    def tearDown(self):
        self.customer_table_info_repo = None
        self.data_table_service = None


    def test_list_tables_with_valid_owner_id(self):
        """
        Should return a list of DataTable objects for a valid owner_id.
        """
        owner_id = 'owner123'
        mock_tables_response_path = self.TEST_RESOURCE_PATH + "expected_tables_for_owner_happy_case.json"
        tables = TestUtils.get_file_content(mock_tables_response_path)
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': tables})

        mock_first_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_table_details_for_first_table_happy_case.json"
        mock_first_table_details = TestUtils.get_file_content(mock_first_table_details_response_path)
        mock_second_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_table_details_for_second_table_happy_case.json"
        mock_second_table_details = TestUtils.get_file_content(mock_second_table_details_response_path)
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_first_table_details,
            mock_second_table_details
        ])

        result = self.data_table_service.list_tables(owner_id)

        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='OriginalTable1')
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='OriginalTable2')
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
        mock_tables_response_path = self.TEST_RESOURCE_PATH + "expected_tables_for_owner_happy_case.json"
        tables = TestUtils.get_file_content(mock_tables_response_path)
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': tables})

        mock_first_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_table_details_for_first_table_with_size_zero.json"
        mock_first_table_details = TestUtils.get_file_content(mock_first_table_details_response_path)
        mock_second_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_table_details_for_second_table_with_size_zero.json"
        mock_second_table_details = TestUtils.get_file_content(mock_second_table_details_response_path)
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_first_table_details,
            mock_second_table_details
        ])

        result = self.data_table_service.list_tables(owner_id)

        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='OriginalTable1')
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_any_call(TableName='OriginalTable2')

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, 'Table1')
        self.assertEqual(result[0].id, 'table123')
        self.assertEqual(result[0].size, 0)
        self.assertEqual(result[1].name, 'Table2')
        self.assertEqual(result[1].id, 'table456')
        self.assertEqual(result[1].size, 0)


    def test_list_tables_with_owner_value_as_none_should_throw_service_exception(self):
        """
        Should propagate ServiceException for owner as none.
        """
        owner_id = None

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.list_tables(owner_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE.value)
        self.assertEqual(context.exception.message, 'owner id cannot be null or empty')


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
        mock_tables_response_path = self.TEST_RESOURCE_PATH + "expected_tables_for_owner_happy_case.json"
        tables = TestUtils.get_file_content(mock_tables_response_path)
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': tables})

        # Mock describe_table to throw a ClientError
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.list_tables(owner_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE.value)
        self.assertEqual(context.exception.message, 'Test Error')
        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')

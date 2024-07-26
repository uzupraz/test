import unittest
from unittest.mock import MagicMock, Mock, patch, call
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import dacite

from tests.test_utils import TestUtils
from model import UpdateTableRequest, UpdateTableResponse, CustomerTableInfo
from repository.customer_table_info_repository import CustomerTableInfoRepository
from service.data_table_service import DataTableService
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

class TestDataTableService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_table/'


    def setUp(self):
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_resource = Mock()
        self.mock_dynamodb_client = Mock()
        self.mock_table = Mock()

        Singleton.clear_instance(CustomerTableInfoRepository)
        with patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_dynamodb_resource') as mock_configure_resource, \
             patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_dynamodb_client') as mock_configure_client, \
             patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_table') as mock_configure_table:

            self.mock_configure_resource = mock_configure_resource
            self.mock_configure_client = mock_configure_client
            self.mock_configure_table = mock_configure_table

            self.mock_configure_resource.return_value = self.mock_dynamodb_resource
            self.mock_configure_client.return_value = self.mock_dynamodb_client
            self.mock_configure_table.return_value = self.mock_table
            self.customer_table_info_repo = CustomerTableInfoRepository(self.app_config, self.aws_config)

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
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_has_calls([
            call(TableName='OriginalTable1'),
            call(TableName='OriginalTable2')
        ], any_order=False)

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
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_has_calls([
            call(TableName='OriginalTable1'),
            call(TableName='OriginalTable2')
        ], any_order=False)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, 'Table1')
        self.assertEqual(result[0].id, 'table123')
        self.assertEqual(result[0].size, 0)
        self.assertEqual(result[1].name, 'Table2')
        self.assertEqual(result[1].id, 'table456')
        self.assertEqual(result[1].size, 0)


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
        while getting the details of the table.
        """
        owner_id = 'owner123'
        mock_tables_response_path = self.TEST_RESOURCE_PATH + "expected_tables_for_owner_happy_case.json"
        tables = TestUtils.get_file_content(mock_tables_response_path)
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': tables})

        # Mock describe_table to throw a ClientError
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.list_tables(owner_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table details')
        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')


    def test_update_table_happy_case(self):
        """
        Test case for updating a customer table description successfully.

        Case: The table item exists and is updated successfully in DynamoDB.
        Expected Result: The method updates the table item and returns the UpdateTableResponse object.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        update_data = UpdateTableRequest(description='Updated description')

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        mock_updated_customer_table_info_path = self.TEST_RESOURCE_PATH + "update_customer_table_item_happy_case.json"
        updated_customer_table_info = TestUtils.get_file_content(mock_updated_customer_table_info_path)
        expected_customer_table_info = dacite.from_dict(CustomerTableInfo, updated_customer_table_info.get('Attributes'))

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.table.update_item.return_value = updated_customer_table_info

        result = self.data_table_service.update_table(owner_id, table_id, update_data)

        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ExpressionAttributeValues={':desc': update_data.description},
            ReturnValues='ALL_NEW'
        )
        self.assertEqual(result, UpdateTableResponse.from_customer_table_info(expected_customer_table_info))


    def test_update_table_throws_service_exception_when_no_item_found_while_retrieving_customer_table_info(self):
        """
        Test case for handling a missing table item during update.

        Case: The table item does not exist in DynamoDB.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        update_data = UpdateTableRequest(description='Updated description')

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_with_empty_result.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.update_table(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table info')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_not_called()


    def test_update_table_throws_service_exception_when_client_error_occurs_while_retrieving_customer_table_info(self):
        """
        Test case for handling a ClientError during retrieval of a customer table item.

        Case: A ClientError occurs during the DynamoDB get_item operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        update_data = UpdateTableRequest(description='Updated description')

        self.mock_table.get_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'get_item')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.update_table(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table info')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_not_called()


    def test_update_table_throws_service_exception_when_client_error_occurs_while_updatig_customer_table_info(self):
        """
        Test case for handling a ClientError during update of a customer table item.

        Case: A ClientError occurs during the DynamoDB update_item operation.
        Expected Result: The method raises a ServiceException indicating failure to update the customer table item.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        update_data = UpdateTableRequest(description='Updated description')

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.mock_table.update_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'update_item')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.update_table(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to update customer table.')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ExpressionAttributeValues={':desc': update_data.description},
            ReturnValues='ALL_NEW'
        )

import unittest
import json
from unittest.mock import MagicMock, Mock, patch, call
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dacite import from_dict
from datetime import datetime

from tests.test_utils import TestUtils
from model import UpdateTableRequest, CustomerTableInfo, BackupJob
from repository.customer_table_info_repository import CustomerTableInfoRepository
from repository.customer_table_repository import CustomerTableRepository
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
        self.mock_dynamodb_backup_client = Mock()
        self.mock_table = Mock()

        Singleton.clear_instance(CustomerTableInfoRepository)
        with patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_dynamodb_resource') as mock_configure_resource, \
             patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_dynamodb_client') as mock_configure_client, \
             patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_backup_client') as mock_configure_backup_client, \
             patch('repository.customer_table_info_repository.CustomerTableInfoRepository._CustomerTableInfoRepository__configure_table') as mock_configure_table:

            self.mock_configure_resource = mock_configure_resource
            self.mock_configure_client = mock_configure_client
            self.mock_configure_backup_client = mock_configure_backup_client
            self.mock_configure_table = mock_configure_table

            self.mock_configure_resource.return_value = self.mock_dynamodb_resource
            self.mock_configure_client.return_value = self.mock_dynamodb_client
            self.mock_configure_backup_client.return_value= self.mock_dynamodb_backup_client
            self.mock_configure_table.return_value = self.mock_table
            self.customer_table_info_repo = CustomerTableInfoRepository(self.app_config, self.aws_config)

        Singleton.clear_instance(CustomerTableRepository)
        with patch('repository.customer_table_repository.CustomerTableRepository._CustomerTableRepository__configure_dynamodb_resource') as mock_customer_table_configure_resource:

            self.mock_customer_table_configure_resource = mock_customer_table_configure_resource

            self.mock_customer_table_configure_resource.return_value = self.mock_dynamodb_resource
            self.customer_table_repo = CustomerTableRepository(self.app_config, self.aws_config)

        Singleton.clear_instance(DataTableService)
        self.data_table_service = DataTableService(self.customer_table_info_repo, self.customer_table_repo)


    def tearDown(self):
        self.customer_table_info_repo = None
        self.customer_table_repo = None
        self.data_table_service = None


    def test_list_tables_with_valid_owner_id(self):
        """
        Should return a list of DataTable objects for a valid owner_id.
        """
        owner_id = 'owner123'
        mock_tables_response_path = self.TEST_RESOURCE_PATH + "expected_tables_for_owner_happy_case.json"
        tables = TestUtils.get_file_content(mock_tables_response_path)
        self.customer_table_info_repo.table.query = MagicMock(return_value={'Items': tables})

        mock_first_dynamodb_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_first_table_happy_case.json"
        mock_first_dynamodb_table_details = TestUtils.get_file_content(mock_first_dynamodb_table_details_response_path)
        mock_second_dynamodb_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_second_table_happy_case.json"
        mock_second_dynamodb_table_details = TestUtils.get_file_content(mock_second_dynamodb_table_details_response_path)
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_first_dynamodb_table_details,
            mock_second_dynamodb_table_details
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

        mock_first_dynamodb_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_first_table_with_size_zero.json"
        mock_first_dynamodb_table_details = TestUtils.get_file_content(mock_first_dynamodb_table_details_response_path)
        mock_second_dynamodb_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_second_table_with_size_zero.json"
        mock_second_dynamodb_table_details = TestUtils.get_file_content(mock_second_dynamodb_table_details_response_path)
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_first_dynamodb_table_details,
            mock_second_dynamodb_table_details
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
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_not_called()


    def test_list_tables_throws_service_exception_while_getting_size_of_the_table(self):
        """
        Returns list of tables from repository sucessfully but propagates ServiceException
        while getting size of the table.
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
        self.assertEqual(context.exception.message, 'Failed to retrieve size of customer table')
        self.customer_table_info_repo.table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')


    def test_update_table_happy_case(self):
        """
        Test case for updating a customer table description successfully.

        Case: The table item exists and is updated successfully in DynamoDB.
        Expected Result: The method updates the customer table description and returns the UpdateTableResponse object.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        update_data = UpdateTableRequest(description='Updated description')

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        mock_updated_customer_table_info_path = self.TEST_RESOURCE_PATH + "updated_customer_table_item_happy_case.json"
        updated_customer_table_info = TestUtils.get_file_content(mock_updated_customer_table_info_path)
        expected_customer_table_info = from_dict(CustomerTableInfo, updated_customer_table_info.get('Attributes'))

        mock_dynamoDB_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_first_table_happy_case.json"
        mock_dynamoDB_table_details = TestUtils.get_file_content(mock_dynamoDB_table_details_response_path)

        for index in expected_customer_table_info.indexes:
            # table size equals index size
            index.size = mock_dynamoDB_table_details['Table']['TableSizeBytes'] / 1024

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.table.update_item.return_value = updated_customer_table_info
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_dynamoDB_table_details
        ])

        result = self.data_table_service.update_description(owner_id, table_id, update_data)

        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ConditionExpression=Attr('owner_id').exists() & Attr('table_id').exists(),
            ExpressionAttributeValues={':desc': update_data.description},
            ReturnValues='ALL_NEW'
        )
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')
        self.assertEqual(result, expected_customer_table_info)


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
            self.data_table_service.update_description(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Customer table item does not exists')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_not_called()
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_not_called()


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
            self.data_table_service.update_description(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table item')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_not_called()
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_not_called()


    def test_update_table_throws_service_exception_when_client_error_occurs_while_updatig_customer_table_info(self):
        """
        Test case for handling a ClientError during update of a customer table description.

        Case: A ClientError occurs during the DynamoDB update_item operation.
        Expected Result: The method raises a ServiceException indicating failure to update the customer table description.
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
            self.data_table_service.update_description(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to update customer table description')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ConditionExpression=Attr('owner_id').exists() & Attr('table_id').exists(),
            ExpressionAttributeValues={':desc': update_data.description},
            ReturnValues='ALL_NEW'
        )
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_not_called()


    def test_update_table_throws_service_exception_when_client_error_occurs_while_retrieving_table_size(self):
        """
        Test case for handling a ClientError during retrieval of table size.

        Case: A ClientError occurs during the DynamoDB describe_table operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve dynamoDB table details.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        update_data = UpdateTableRequest(description='Updated description')

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        mock_updated_customer_table_info_path = self.TEST_RESOURCE_PATH + "updated_customer_table_item_happy_case.json"
        updated_customer_table_info = TestUtils.get_file_content(mock_updated_customer_table_info_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.table.update_item.return_value = updated_customer_table_info
        self.customer_table_info_repo.dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.update_description(owner_id, table_id, update_data)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve size of customer table')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ConditionExpression=Attr('owner_id').exists() & Attr('table_id').exists(),
            ExpressionAttributeValues={':desc': update_data.description},
            ReturnValues='ALL_NEW'
        )
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')


    def test_get_table_info_with_one_index_happy_case(self):
        """
        Test case for retrieving table info with one index.

        Case: The table item with one index exists in DynamoDB.
        Expected Result: The method returns the CustomerTableInfo object with one index.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_one_index.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        mock_dynamoDB_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_first_table_happy_case.json"
        mock_dynamoDB_table_details = TestUtils.get_file_content(mock_dynamoDB_table_details_response_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_dynamoDB_table_details
        ])

        expected_expected_customer_table_info = from_dict(CustomerTableInfo, customer_table_info_item.get('Item'))
        for index in expected_expected_customer_table_info.indexes:
            # table size equals index size
            index.size = mock_dynamoDB_table_details['Table']['TableSizeBytes'] / 1024

        result = self.data_table_service.get_table_info(owner_id, table_id)

        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')
        self.assertEqual(result, expected_expected_customer_table_info)


    def test_get_table_info_with_two_indeces_happy_case(self):
        """
        Test case for retrieving table info with two indexes.

        Case: The table item with two indices exists in DynamoDB.
        Expected Result: The method returns the CustomerTableInfo object with two indexes.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_two_indexes.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        mock_dynamoDB_table_details_response_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_first_table_happy_case.json"
        mock_dynamoDB_table_details = TestUtils.get_file_content(mock_dynamoDB_table_details_response_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_client.describe_table = MagicMock(side_effect=[
            mock_dynamoDB_table_details
        ])

        expected_expected_customer_table_info = from_dict(CustomerTableInfo, customer_table_info_item.get('Item'))
        for index in expected_expected_customer_table_info.indexes:
            # table size equals index size
            index.size = mock_dynamoDB_table_details['Table']['TableSizeBytes'] / 1024

        result = self.data_table_service.get_table_info(owner_id, table_id)

        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')
        self.assertEqual(result, expected_expected_customer_table_info)


    def test_get_table_info_throws_service_exception_when_no_item_found_while_retrieving_customer_table_info(self):
        """
        Test case for handling a missing table item.

        Case: The table item does not exist in DynamoDB.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_with_empty_result.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_info(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Customer table item does not exists')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_client.describe_table_assert_not_called()


    def test_get_table_info_throws_service_exception_when_client_error_occurs_while_retrieving_customer_table_info(self):
        """
        Test case for handling a ClientError during retrieval.

        Case: A ClientError occurs during the DynamoDB get_item operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        self.mock_table.get_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'get_item')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_info(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table item')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_client.describe_table_assert_not_called()


    def test_get_table_info_throws_service_exception_when_client_error_occurs_while_retrieving_table_size(self):
        """
        Test case for handling a ClientError during retrieval of dynamoDB table details.

        Case: A ClientError occurs during the DynamoDB describe_table operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the table size.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_one_index.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_info(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve size of customer table')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_client.describe_table.assert_called_once_with(TableName='OriginalTable1')


    def test_get_table_backup_jobs_happy_case(self):
        """
        Test case for successfully retrieving table backup jobs.

        Case: The table info and backup jobs are correctly retrieved.
        Expected Result: The method returns the expected list of backup job.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_one_index.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        table_name = customer_table_info_item.get('Item').get('original_table_name')

        mock_response_path = self.TEST_RESOURCE_PATH + "expected_backup_jobs_for_table_happy_case.json"
        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_backup_jobs = mock_response['BackupJobs']
        for mock_backup_job in mock_backup_jobs:
            mock_backup_job['CreationDate'] = datetime.strptime(mock_backup_job['CreationDate'], '%Y-%m-%d %H:%M:%S%z')

        # Sort the backup jobs by `CreationDate` in descending order
        sorted_backup_jobs = sorted(
            mock_backup_jobs,
            key=lambda job: job['CreationDate'],
            reverse=True
        )
        # Return the latest 10 backup jobs
        latest_backup_jobs = sorted_backup_jobs[:10]

        expected_backup_jobs = []
        for backup_job in latest_backup_jobs:
            creation_time = backup_job['CreationDate'].strftime('%Y-%m-%d %H:%M:%S%z')
            expected_backup_jobs.append(BackupJob(id=backup_job['BackupJobId'],
                                                        name=table_name + '_' + backup_job['CreationDate'].strftime('%Y%m%d%H%M%S'),
                                                        creation_time=creation_time,
                                                        size=backup_job['BackupSizeInBytes'] / 1024))

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.return_value = mock_response

        result = self.data_table_service.get_table_backup_jobs(owner_id, table_id)

        self.customer_table_info_repo.table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=customer_table_info_item.get('Item').get('table_arn'))
        self.assertEqual(result, expected_backup_jobs)


    def test_get_table_backup_jobs_should_return_latest_ten_backup_jobs(self):
        """
        Test case for successfully retrieving table backup jobs.
        Should return the latest 10 backup jobs of the table when the list_backup_jobs api provides more than 10 backup jobs in respnose.
        The list_backup_jobs returns backup jobs for maximum 30 days. Since our backup is schedule daily so it might return maximum
        30 backup jobs in response. Since our backup stores only for 10 days, so latest 10 backup jobs is retrieved.

        Case: The table info and backup jobs are correctly retrieved.
        Expected Result: The method returns the expected list of latest 10 backup job.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_one_index.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        table_name = customer_table_info_item.get('Item').get('original_table_name')

        mock_response_path = self.TEST_RESOURCE_PATH + "backup_jobs_with_length_more_than_ten.json"
        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_backup_jobs = mock_response['BackupJobs']
        for mock_backup_job in mock_backup_jobs:
            mock_backup_job['CreationDate'] = datetime.strptime(mock_backup_job['CreationDate'], '%Y-%m-%d %H:%M:%S%z')

        # Sort the backup jobs by `CreationDate` in descending order
        sorted_backup_jobs = sorted(
            mock_backup_jobs,
            key=lambda job: job['CreationDate'],
            reverse=True
        )
        # Return the latest 10 backup jobs
        latest_backup_jobs = sorted_backup_jobs[:10]

        expected_backup_jobs = []
        for backup_job in latest_backup_jobs:
            creation_time = backup_job['CreationDate'].strftime('%Y-%m-%d %H:%M:%S%z')
            expected_backup_jobs.append(BackupJob(id=backup_job['BackupJobId'],
                                                        name=table_name + '_' + backup_job['CreationDate'].strftime('%Y%m%d%H%M%S'),
                                                        creation_time=creation_time,
                                                        size=backup_job['BackupSizeInBytes'] / 1024))

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.return_value = mock_response

        result = self.data_table_service.get_table_backup_jobs(owner_id, table_id)

        self.customer_table_info_repo.table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=customer_table_info_item.get('Item').get('table_arn'))
        self.assertEqual(result, expected_backup_jobs)
        self.assertEqual(10, len(result))


    def test_get_table_backup_jobs_throws_service_exception_when_no_item_found_while_retrieving_customer_table_info(self):
        """
        Test case for handling a missing table item.

        Case: The table item does not exist in DynamoDB.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_with_empty_result.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_backup_jobs(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Customer table item does not exists')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_not_called()


    def test_get_table_backup_jobs_details_throws_service_exception_when_client_error_occurs_while_retrieving_customer_table_info(self):
        """
        Test case for handling a ClientError during retrieval.

        Case: A ClientError occurs during the DynamoDB get_item operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        self.mock_table.get_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'get_item')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_backup_jobs(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table item')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_not_called()


    def test_get_table_backup_jobs_should_return_empty_list_when_no_backup_jobs_available_for_the_table(self):
        """
        Test case for handling no backup jobs available for the table.

        Case: The table has no backup jobs.
        Expected Result: The method returns an empty list.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_one_index.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.return_value = {'BackupJobs': [], 'NextToken': None}

        result = self.data_table_service.get_table_backup_jobs(owner_id, table_id)

        self.customer_table_info_repo.table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=customer_table_info_item.get('Item').get('table_arn'))
        self.assertEqual(result, [])


    def test_get_table_backup_jobs_throws_service_exception_when_backup_client_throws_error_while_retrieving_backup_jobs(self):
        """
        Test case for handling a ClientError during backup jobs retrieval.

        Case: A ClientError occurs during the back up client list_backups operation. The error might ocuur due to several reasons
        for example, when invalid table arn is provided.

        Expected Result: The method raises a ServiceException indicating indicating failure to retreive backup jobs.
        """
        owner_id = 'owner123'
        table_id = 'table123'

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "expected_table_details_with_one_index.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'list_backups')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_backup_jobs(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve backup jobs of customer table')
        self.customer_table_info_repo.table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=customer_table_info_item.get('Item').get('table_arn'))


    def test_get_table_items_success_case(self):
        """
        Test case for retrieving table items successfully.

        Case: The table items is fetched successfully.
        Expected Result: The method returns a CustomerTableItem object with the items and pagination which is CustomerTableItemPagination.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        size = 10
        last_evaluated_key = None

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})

        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_table_items_happy_case.json"
        table_content_items = TestUtils.get_file_content(mock_table_items_path)

        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))
        self.customer_table_repo.get_table_items = MagicMock(return_value=(table_content_items, None))

        result = self.data_table_service.get_table_items(owner_id, table_id, size, last_evaluated_key)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.get_table_items.assert_called_once_with(
            table_name=customer_table_info_item['original_table_name'],
            limit=size,
            exclusive_start_key=None
        )

        self.assertEqual(len(result.items), len(table_content_items))
        self.assertEqual(result.pagination.size, size)
        self.assertIsNone(result.pagination.last_evaluated_key)


    def test_get_table_items_with_last_evaluated_key(self):
        """
        Test case for retrieving table items with last_evaluated_key.

        Case: The table items is fetched successfully with a last_evaluated_key.
        Expected Result: The method returns a CustomerTableItem object with the items and pagination which is CustomerTableItemPagination.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        size = 10
        last_evaluated_key = 'eyJzb21lX2tleSI6ICJzb21lX3ZhbHVlIn0='

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        mock_table_content_items_path = self.TEST_RESOURCE_PATH + "get_table_items_happy_case.json"
        table_content_items = TestUtils.get_file_content(mock_table_content_items_path)

        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))
        self.customer_table_repo.get_table_items = MagicMock(return_value=(table_content_items, {"next_key": "next_value"}))

        result = self.data_table_service.get_table_items(owner_id, table_id, size, last_evaluated_key)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.get_table_items.assert_called_once_with(
            table_name=customer_table_info_item['original_table_name'],
            limit=size,
            exclusive_start_key=json.loads(TestUtils.decode_base64(last_evaluated_key))
        )

        self.assertEqual(len(result.items), len(table_content_items))
        self.assertEqual(result.pagination.size, size)
        self.assertEqual(result.pagination.last_evaluated_key, TestUtils.encode_to_base64(json.dumps({"next_key": "next_value"})))


    def test_get_table_items_gets_service_exception_when_customer_table_info_not_found(self):
        """
        Test case for handling a missing table item during retrieval of table items.

        Case: The table item does not exist in DynamoDB.
        Expected Result: The method gets a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        size = 10
        last_evaluated_key = None

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_with_empty_result.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)

        self.customer_table_info_repo.table.get_item.return_value = customer_table_info_item

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_items(owner_id, table_id, size, last_evaluated_key)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Customer table item does not exists')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})
        self.customer_table_info_repo.dynamodb_backup_client.list_backup_jobs.assert_not_called()


    def test_get_table_items_throws_service_exception_when_client_error_occurs(self):
        """
        Test case for handling a ClientError during retrieval of table items.

        Case: A ClientError occurs during the DynamoDB get_table_items operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the table items.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        size = 10
        last_evaluated_key = None

        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})

        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        mock_dynamodb_resource_table = MagicMock()
        self.customer_table_repo.dynamodb_resource.Table.return_value = mock_dynamodb_resource_table
        mock_dynamodb_resource_table.scan.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'get_table_content')

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.get_table_items(owner_id, table_id, size, last_evaluated_key)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve table items')
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        mock_dynamodb_resource_table.scan.assert_called_once_with(Limit=size)


    def test_create_item_success_case(self):
        """
        Test case for creating an item into a table successfully.

        Case: The item is valid and the table exists.
        Expected Result: The item is inserted successfully and returned with an expiration date.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        item = {'partition_key': 'partition_key', 'sort_key': 'sort_key', 'data': 'sample data'}

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))
        
        # Mock the create_item response
        self.customer_table_repo.create_item = MagicMock(return_value=item)

        # Call the create_item method
        result = self.data_table_service.create_item(owner_id, table_id, item)

        # Assert that the repository methods were called with the correct arguments
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.create_item.assert_called_once_with(
            table_name='OriginalTable1',
            item={
                'partition_key': 'partition_key', 
                'sort_key': 'sort_key',
                'data': 'sample data',
                'expiration_date': result['expiration_date']
            }
        )

        # Assert the result
        self.assertEqual(result['partition_key'], 'partition_key')
        self.assertEqual(result['sort_key'], 'sort_key')
        self.assertEqual(result['data'], 'sample data')
        self.assertIn('expiration_date', result)


    def test_create_item_raises_exception_on_invalid_item(self):
        """
        Test case for handling invalid item input.

        Case: The item does not have string keys.
        Expected Result: The method raises a ServiceException indicating invalid input data.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        item = ['invalid', 'item']  # Invalid item type (not a dict)

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.create_item(owner_id, table_id, item)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Invalid input data. Expected a JSON object with string keys.')


    def test_create_item_raises_exception_on_missing_partition_key(self):
        """
        Test case for handling item with missing 'partition' key.

        Case: The item does not contain the 'partition' key.
        Expected Result: The method raises a ServiceException indicating missing 'partition' key.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        item = {'data': 'sample data'}  # Missing 'partition' key

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.create_item(owner_id, table_id, item)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(
            owner_id,
            table_id
        )
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Missing partition key in input item')


    def test_create_item_raises_exception_on_missing_sort_key(self):
        """
        Test case for handling item with missing 'sort' key.

        Case: The item does not contain the 'sort' key.
        Expected Result: The method raises a ServiceException indicating missing 'sort' key.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        item = {'partition_key': 'partition_key', 'data': 'sample data'}  # Missing 'sort' key

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.create_item(owner_id, table_id, item)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(
            owner_id,
            table_id
        )
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Missing sort key in input item')


    def test_create_item_raises_exception_on_table_not_found(self):
        """
        Test case for handling the scenario where the table is not found.

        Case: The table does not exist in the repository.
        Expected Result: The method raises a ServiceException indicating the table was not found.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        item = {'id': 'item001', 'data': 'sample data'}

        # Mock the get_table_item to raise an exception
        self.customer_table_info_repo.get_table_item = MagicMock(side_effect=ServiceException(404, ServiceStatus.FAILURE, 'Table not found'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.create_item(owner_id, table_id, item)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(
            owner_id,
            table_id
        )
        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Table not found')


    def test_delete_item_success_case(self):
        """
        Test case for successfully deleting an item from the table.
        
        Case: The table exists and the item is successfully deleted.
        Expected Result: The item is deleted without any exceptions.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        customer_table_info_item['sort_key'] = None
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the delete_item method to not raise any exception
        self.customer_table_repo.delete_item = MagicMock(return_value=None)

        # Call the delete_item method
        self.data_table_service.delete_item(owner_id, table_id, partition_key_value)

        # Assert that the repository methods were called with the correct arguments
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.delete_item.assert_called_once_with(
            table_name='OriginalTable1',
            key={'partition_key': partition_key_value}
        )


    def test_delete_item_with_partition_key_and_sort_key_case(self):
        """
        Test case for successfully deleting an item from the table with both partition & sort key present.
        
        Case: The table exists and the item is successfully deleted.
        Expected Result: The item is deleted without any exceptions.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'
        sort_key_value = 'sort001'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the delete_item method to not raise any exception
        self.customer_table_repo.delete_item = MagicMock(return_value=None)

        # Call the delete_item method
        self.data_table_service.delete_item(owner_id, table_id, partition_key_value, sort_key_value)

        # Assert that the repository methods were called with the correct arguments
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.delete_item.assert_called_once_with(
            table_name='OriginalTable1',
            key={'partition_key': partition_key_value, 'sort_key': sort_key_value}
        )


    def test_delete_item_raises_exception_on_table_not_found(self):
        """
        Test case for handling the scenario where the table is not found.

        Case: The table does not exist in the repository.
        Expected Result: The method raises a ServiceException indicating the table was not found.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        key = 'item001'

        # Mock the get_table_item to raise an exception
        self.customer_table_info_repo.get_table_item =  MagicMock(side_effect=ServiceException(404, ServiceStatus.FAILURE, 'Table not found'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.delete_item(owner_id, table_id, key)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Table not found')


    def test_delete_item_raises_exception_when_sort_key_is_present_but_not_provided(self):
        """
        Test case for handling the scenario where item deletion fails when sort key is not provided but exist in customer info table.

        Case: The deletion of the item fails due to sort key missing failure.
        Expected Result: The method raises a ServiceException indicating the deletion failure.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.delete_item(owner_id, table_id, partition_key_value)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Sort key is required but not provided in input')


    def test_delete_item_raises_exception_on_deletion_failure(self):
        """
        Test case for handling the scenario where item deletion fails.

        Case: The deletion of the item fails due to a client error.
        Expected Result: The method raises a ServiceException indicating the deletion failure.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        customer_table_info_item['sort_key'] = None
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the delete_item method to raise a ServiceException
        self.customer_table_repo.delete_item =  MagicMock(side_effect=ServiceException(500, ServiceStatus.FAILURE, 'Failed to delete item from table'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.delete_item(owner_id, table_id, partition_key_value)

        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.delete_item.assert_called_once_with(
            table_name='OriginalTable1',
            key={'partition_key': partition_key_value}
        )
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to delete item from table')


    def test_query_item_with_partition_key_only(self):
        """
        Test querying items using only the partition key.
        
        Case: Only partition key is provided.
        Expected Result: Items matching the partition key are returned.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        customer_table_info_item['sort_key'] = None
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the query_item method response
        mock_items = [{'partition_key': partition_key_value, 'data': 'value1'}]
        self.customer_table_repo.query_item = MagicMock(return_value=mock_items)

        # Call the query_item method
        result = self.data_table_service.query_item(owner_id, table_id, partition_key_value)

        # Assertions
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.query_item.assert_called_once_with(
            table_name='OriginalTable1',
            partition_key=(customer_table_info_item['partition_key'], partition_key_value),
            sort_key=None,
            filters=None
        )
        self.assertEqual(result, mock_items)


    def test_query_item_with_partition_and_sort_key(self):
        """
        Test querying items using both partition and sort keys.
        
        Case: Both partition and sort keys are provided.
        Expected Result: Items matching the partition and sort keys are returned.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'
        sort_key_value = 'sort001'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the query_item method response
        mock_items = [{'partition_key': partition_key_value, 'sort_key': sort_key_value, 'data': 'value1'}]
        self.customer_table_repo.query_item = MagicMock(return_value=mock_items)

        # Call the query_item method
        result = self.data_table_service.query_item(owner_id, table_id, partition_key_value, sort_key_value)

        # Assertions
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.query_item.assert_called_once_with(
            table_name='OriginalTable1',
            partition_key=(customer_table_info_item['partition_key'], partition_key_value),
            sort_key=(customer_table_info_item['sort_key'], sort_key_value),
            filters=None
        )
        self.assertEqual(result, mock_items)


    def test_query_item_with_filters(self):
        """
        Test querying items using partition and sort keys with additional filters.
        
        Case: Partition and sort keys are provided along with attribute filters.
        Expected Result: Items matching the partition, sort keys, and filters are returned.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'item001'
        sort_key_value = 'sort001'
        attribute_filters = {'status': 'active'}

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the query_item method response
        mock_items = [{'partition_key': partition_key_value, 'sort_key': sort_key_value, 'status': 'active', 'data': 'value1'}]
        self.customer_table_repo.query_item = MagicMock(return_value=mock_items)

        # Call the query_item method
        result = self.data_table_service.query_item(owner_id, table_id, partition_key_value, sort_key_value, attribute_filters)

        # Assertions
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.query_item.assert_called_once_with(
            table_name='OriginalTable1',
            partition_key=(customer_table_info_item['partition_key'], partition_key_value),
            sort_key=(customer_table_info_item['sort_key'], sort_key_value),
            filters=attribute_filters
        )
        self.assertEqual(result, mock_items)


    def test_query_item_no_results(self):
        """
        Test querying items where no matching items are found.
        
        Case: No items match the provided keys and filters.
        Expected Result: An empty list is returned.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        partition_key_value = 'nonexistent'

        # Mock the customer table info repository response
        mock_customer_table_info_item_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        customer_table_info_item = TestUtils.get_file_content(mock_customer_table_info_item_path)
        customer_table_info_item = customer_table_info_item.get("Item", {})
        self.customer_table_info_repo.get_table_item = MagicMock(return_value=from_dict(CustomerTableInfo, customer_table_info_item))

        # Mock the query_item method to return no items
        self.customer_table_repo.query_item = MagicMock(return_value=[])

        # Call the query_item method
        result = self.data_table_service.query_item(owner_id, table_id, partition_key_value)

        # Assertions
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.customer_table_repo.query_item.assert_called_once_with(
            table_name='OriginalTable1',
            partition_key=(customer_table_info_item['partition_key'], partition_key_value),
            sort_key=None,
            filters=None
        )
        self.assertEqual(result, [])


    def test_query_item_raises_exception_on_table_not_found(self):
        """
        Test handling the scenario where the table is not found in the repository.
        
        Case: The table does not exist in the repository.
        Expected Result: A ServiceException is raised indicating the table was not found.
        """
        owner_id = 'owner123'
        table_id = 'nonexistent_table'
        partition_key_value = 'item001'

        # Mock the get_table_item to raise an exception
        self.customer_table_info_repo.get_table_item = MagicMock(side_effect=ServiceException(404, ServiceStatus.FAILURE, 'Table not found'))

        with self.assertRaises(ServiceException) as context:
            self.data_table_service.query_item(owner_id, table_id, partition_key_value)

        # Assertions
        self.customer_table_info_repo.get_table_item.assert_called_once_with(owner_id, table_id)
        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Table not found')
import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dacite import from_dict
from datetime import datetime

from tests.test_utils import TestUtils
from model import  CustomerTableInfo
from repository.customer_table_info_repository import CustomerTableInfoRepository, BackupDetail
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton

class TestCustomerTableInfoRepository(unittest.TestCase):


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
            self.mock_configure_backup_client.return_value = self.mock_dynamodb_backup_client
            self.mock_configure_table.return_value = self.mock_table

            self.customer_table_info_repo = CustomerTableInfoRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.customer_table_info_repo = None


    def test_get_tables_for_owner_happy_case(self):
        """
        Should return a list of tables for a valid owner_id.
        """
        owner_id = 'owner123'
        mock_response_path = self.TEST_RESOURCE_PATH + "expected_tables_for_owner_happy_case.json"
        expected_items = TestUtils.get_file_content(mock_response_path)
        expected_tables = []
        for expected_item in expected_items:
            expected_table = from_dict(CustomerTableInfo, expected_item)
            expected_tables.append(expected_table)

        self.mock_table.query.return_value = {'Items': expected_items}

        result = self.customer_table_info_repo.get_tables_for_owner(owner_id)

        self.mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(result, expected_tables)


    def test_get_tables_for_owner_should_return_empty_tables(self):
        """
        Should return an empty list when there are no tables for the specified owner_id.
        """
        owner_id = 'owner123'
        self.mock_table.query.return_value = {'Items': []}

        result = self.customer_table_info_repo.get_tables_for_owner(owner_id)

        self.mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(result, [])


    def test_get_tables_for_owner_with_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        owner_id = 'owner123'
        self.mock_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_tables_for_owner(owner_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer tables')
        self.mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))


    def test_get_table_size_happy_case(self):
        """
        Should return the correct size of the dynamoDB table.
        """
        table_name = 'originalTable1'
        mock_dynamodb_table_details_path = self.TEST_RESOURCE_PATH + "expected_dynamodb_table_details_for_first_table_happy_case.json"
        mock_dynamodb_table_details = TestUtils.get_file_content(mock_dynamodb_table_details_path)
        self.mock_dynamodb_client.describe_table.return_value = mock_dynamodb_table_details

        result = self.customer_table_info_repo.get_table_size(table_name)

        self.mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)
        self.assertEqual(result, mock_dynamodb_table_details['Table'] ['TableSizeBytes']/1024)


    def test_get_table_size_with_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        table_name = 'originalTable1'
        self.mock_dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_size(table_name)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve size of customer table')
        self.mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)


    def test_get_table_size_when_table_not_found_throws_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ResourceNotFoundException.
        """
        table_name = 'nonExistentTable'
        self.mock_dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Requested resource not found'}, 'ResponseMetadata': {'HTTPStatusCode': 404}}, 'describe_table')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_size(table_name)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve size of customer table')
        self.mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)


    def test_get_table_item_happy_case(self):
        """
        Test case for retrieving a customer table item successfully.

        Case: The table item exists and is retrieved successfully from DynamoDB.
        Expected Result: The method returns the CustomerTableInfo object corresponding to the retrieved item.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        mock_response_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_happy_case.json"
        expected_item = TestUtils.get_file_content(mock_response_path)
        self.mock_table.get_item.return_value = expected_item

        result = self.customer_table_info_repo.get_table_item(owner_id, table_id)

        self.assertEqual(result, from_dict(CustomerTableInfo, expected_item.get('Item')))
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})


    def test_get_table_item_throws_service_exception_when_no_item_found(self):
        """
        Test case for retrieving a customer table item that does not exist.

        Case: The table item does not exist in DynamoDB.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table info.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        mock_response_path = self.TEST_RESOURCE_PATH + "get_customer_table_item_with_empty_result.json"
        expected_item = TestUtils.get_file_content(mock_response_path)
        self.mock_table.get_item.return_value = expected_item

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_item(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Customer table item does not exists')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})


    def test_get_table_item_with_service_exception(self):
        """
        Test case for handling an exception during retrieval of a customer table item.

        Case: A ClientError occurs during the DynamoDB get_item operation.
        Expected Result: The method raises a ServiceException indicating failure to retrieve the customer table item.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        self.mock_table.get_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'get_item')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_item(owner_id, table_id)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve customer table item')
        self.mock_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'table_id': table_id})


    def test_update_description_happy_case(self):
        """
        Test case for updating description of customer table successfully.

        Case: The table item exists and is updated successfully in DynamoDB.
        Expected Result: The method updates the description and returns the updated CustomerTableInfo object.
        """
        Customer_table_info = CustomerTableInfo(
            owner_id='owner123', table_id='table123',
            table_name='test_table_name', original_table_name='OriginalTable1',
            description='Test description', partition_key='partition_key', sort_key='sort_key')

        mock_response_path = self.TEST_RESOURCE_PATH + "updated_customer_table_item_happy_case.json"
        expected_item = TestUtils.get_file_content(mock_response_path)
        self.mock_table.update_item.return_value = expected_item

        result = self.customer_table_info_repo.update_description(Customer_table_info)

        self.mock_table.update_item.assert_called_once_with(
            Key={'owner_id': Customer_table_info.owner_id, 'table_id': Customer_table_info.table_id},
            UpdateExpression='SET description = :desc',
            ConditionExpression=Attr('owner_id').exists() & Attr('table_id').exists(),
            ExpressionAttributeValues={':desc': Customer_table_info.description},
            ReturnValues='ALL_NEW'
        )
        self.assertEqual(result, from_dict(CustomerTableInfo, expected_item.get('Attributes')))


    def test_update_table_with_client_error(self):
        """
        Test case for handling an exception during update of customer table description.

        Case: A ClientError occurs during the DynamoDB update_item operation.
        Expected Result: The method raises a ServiceException indicating failure to update the customer table description.
        """
        Customer_table_info = CustomerTableInfo(
            owner_id='owner123', table_id='table123',
            table_name='test_table_name', original_table_name='OriginalTable1',
            description='Test description', partition_key='partition_key', sort_key='sort_key')
        self.mock_table.update_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'update_item')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.update_description(Customer_table_info)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to update customer table description')
        self.mock_table.update_item.assert_called_once_with(
            Key={'owner_id': Customer_table_info.owner_id, 'table_id': Customer_table_info.table_id},
            UpdateExpression='SET description = :desc',
            ConditionExpression=Attr('owner_id').exists() & Attr('table_id').exists(),
            ExpressionAttributeValues={':desc': Customer_table_info.description},
            ReturnValues="ALL_NEW"
        )


    def test_get_table_backup_details_happy_case(self):
        """
        Should return the correct backup details of the table.
        """
        table_name = 'originalTable1'
        table_arn = 'table_arn'
        mock_response_path = self.TEST_RESOURCE_PATH + "expected_backup_details_for_table_happy_case.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        expected_backup_details = []
        for backup_job in mock_response['BackupJobs']:
            backup_job['CreationDate'] = datetime.strptime(backup_job['CreationDate'], '%Y-%m-%d %H:%M:%S%z')
            expected_backup_details.append(BackupDetail(id=backup_job['BackupJobId'],
                                                        name=table_name + '_' + backup_job['CreationDate'].strftime('%Y%m%d%H%M%S'),
                                                        creation_time=backup_job['CreationDate'].strftime('%Y-%m-%d %H:%M:%S%z'),
                                                        size=backup_job['BackupSizeInBytes']/1024))
        self.mock_dynamodb_backup_client.list_backup_jobs.return_value = mock_response

        result = self.customer_table_info_repo.get_table_backup_details(table_name, table_arn)

        self.mock_dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=table_arn)
        self.assertEqual(result, expected_backup_details)


    def test_get_table_backup_details_return_empty_backup_details(self):
        """
        Should return an empty backup details when there are no backup details avialbale for the table.
        """
        table_name = 'originalTable1'
        table_arn = 'table_arn'
        self.mock_dynamodb_backup_client.list_backup_jobs.return_value = {'BackupJobs': [], 'NextToken': None}

        result = self.customer_table_info_repo.get_table_backup_details(table_name, table_arn)

        self.mock_dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=table_arn)
        self.assertEqual(result, [])


    def test_get_table_backup_details_with_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        table_name = 'originalTable1'
        table_arn = 'table_arn'
        self.mock_dynamodb_backup_client.list_backup_jobs.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'list_backup_jobs')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_backup_details(table_name, table_arn)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve backup details of customer table')
        self.mock_dynamodb_backup_client.list_backup_jobs.assert_called_once_with(ByResourceArn=table_arn)

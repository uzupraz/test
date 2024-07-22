import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import dacite

from tests.test_utils import TestUtils
from model import  CustomerTableInfo
from repository.customer_table_info_repository import CustomerTableInfoRepository
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
            expected_table = dacite.from_dict(CustomerTableInfo, expected_item)
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
        self.assertEqual(context.exception.message, 'Failed to retrieve tables')
        self.mock_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))


    def test_get_table_details_happy_case(self):
        """
        Should return the correct details of the table.
        """
        table_name = 'originalTable1'
        mock_response_path = self.TEST_RESOURCE_PATH + "expected_table_details_for_first_table_happy_case.json"
        expected_response = TestUtils.get_file_content(mock_response_path)
        self.mock_dynamodb_client.describe_table.return_value = expected_response

        result = self.customer_table_info_repo.get_table_details(table_name)

        self.mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)
        self.assertEqual(result, expected_response)


    def test_get_table_details_with_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        table_name = 'originalTable1'
        self.mock_dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'describe_table')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_details(table_name)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve table details')
        self.mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)


    def test_get_table_details_when_table_not_found_throws_service_exception(self):
        """
        Should propagate ServiceException when DynamoDB throws a ResourceNotFoundException.
        """
        table_name = 'nonExistentTable'
        self.mock_dynamodb_client.describe_table.side_effect = ClientError(
            {'Error': {'Message': 'Requested resource not found'}, 'ResponseMetadata': {'HTTPStatusCode': 404}}, 'describe_table')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.get_table_details(table_name)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to retrieve table details')
        self.mock_dynamodb_client.describe_table.assert_called_once_with(TableName=table_name)


    def test_update_table_happy_case(self):
        """
        Should update the table description successfully.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        description = 'Updated description'

        self.customer_table_info_repo.update_table(owner_id, table_id, description)

        self.mock_table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ExpressionAttributeValues={':desc': description}
        )


    def test_update_table_with_client_error(self):
        """
        Should propagate ServiceException when DynamoDB throws a ClientError.
        """
        owner_id = 'owner123'
        table_id = 'table123'
        description = 'Updated description'
        self.mock_table.update_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'update_item')

        with self.assertRaises(ServiceException) as context:
            self.customer_table_info_repo.update_table(owner_id, table_id, description)

        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.message, 'Failed to update description of table.')
        self.mock_table.update_item.assert_called_once_with(
            Key={'owner_id': owner_id, 'table_id': table_id},
            UpdateExpression='SET description = :desc',
            ExpressionAttributeValues={':desc': description}
        )

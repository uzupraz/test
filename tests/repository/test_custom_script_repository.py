import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from model import CustomScript
from repository.custom_script_repository import CustomScriptRepository
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton
from tests.test_utils import TestUtils


class TestCustomScriptRepository(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/custom_script/'


    def setUp(self):
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_table = Mock()

        Singleton.clear_instance(CustomScriptRepository)
        with patch('repository.custom_script_repository.CustomScriptRepository._CustomScriptRepository__configure_dynamodb') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            self.mock_configure_resource.return_value = self.mock_dynamodb_table
            self.custom_script_repository = CustomScriptRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.custom_script_repository = None
        self.mock_configure_resource = None


    def test_get_owner_custom_scripts_success_case(self):
        owner_id = 'TEST_OWNER_ID'

        # Mock response from DynamoDB query
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_owner_custom_scripts_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        items = self.custom_script_repository.get_owner_custom_scripts(owner_id)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(type(items), list)
        self.assertEqual(len(items), len(mock_items))
        self.assertEqual(type(items[0]), CustomScript)


    def test_get_owner_custom_scripts_throws_client_exception(self):
        owner_id = 'TEST_OWNER_ID'

        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.custom_script_repository.get_owner_custom_scripts(owner_id)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(KeyConditionExpression=Key('owner_id').eq(owner_id))
        self.assertEqual(e.exception.message, "Failed to retrieve owner custom script")
        self.assertEqual(e.exception.status_code, 400)

    
    def test_get_custom_script_success_case(self):
        owner_id = 'TEST_OWNER_ID'
        script_id = 'TEST_SCRIPT_ID'

        # Mock response from DynamoDB query
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_custom_script_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.get_item.return_value = {
            'Item': mock_items,
        }

        # Call the method under test
        item = self.custom_script_repository.get_custom_script(owner_id, script_id)

        # Assertions
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'script_id': script_id})
        self.assertIsInstance(item, CustomScript)


    def test_get_custom_script_throws_client_exception(self):
        owner_id = 'TEST_OWNER_ID'
        script_id = 'TEST_SCRIPT_ID'

        self.mock_dynamodb_table.get_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'get_item'
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.custom_script_repository.get_custom_script(owner_id, script_id)

        # Assertions
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={'owner_id': owner_id, 'script_id': script_id})
        self.assertEqual(e.exception.message, "Failed to retrieve custom script")
        self.assertEqual(e.exception.status_code, 400)

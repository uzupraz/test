import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from repository import ChatRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from utils import Singleton
from model import ChatSession, ChatMessage


class TestChatRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/chatbot/'

    
    def setUp(self) -> None:
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_table = Mock()

        Singleton.clear_instance(ChatRepository)
        with patch('repository.chatbot.chat_repository.ChatRepository._ChatRepository__configure_dynamodb') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            mock_configure_resource.return_value = self.mock_dynamodb_table
            self.chat_repository = ChatRepository(self.app_config, self.aws_config)


    def tearDown(self) -> None:
        self.chat_repository = None
        self.mock_configure_resource = None


    def test_get_user_chats_success_case(self):
        user_id = "TEST_USER_ID"

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_user_chats_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        chats = self.chat_repository.get_user_chats(user_id)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        self.assertEqual(type(chats), list)
        self.assertEqual(len(chats), len(mock_items))
        self.assertEqual(type(chats[0]), ChatSession)


    def test_get_user_chats_throws_client_exception(self):
        user_id = "TEST_USER_ID"

        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )
        
        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_user_chats(user_id)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        self.assertEqual(e.exception.status_code, 400)


    def test_get_chat_messages_success(self):
        chat_id = "TEST_CHAT_ID"
        limit = 10
        exclusive_start_key = None

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_chat_messages_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)
        mock_last_evaluated_key = {"key": "value"}

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': mock_last_evaluated_key
        }

        # Call the method under test
        chat_response = self.chat_repository.get_chat_messages(chat_id, limit, exclusive_start_key)

        items = chat_response.messages
        last_evaluated_key = chat_response.last_evaluated_key

        # Assertions
        self.assertEqual(type(items), list)
        self.assertEqual(len(items), len(mock_items))
        self.assertEqual(type(items[0]), ChatMessage)  
        self.assertEqual(last_evaluated_key, mock_last_evaluated_key)

        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(chat_id),
            Limit=limit
        )


    def test_get_chat_messages_throws_client_exception(self):
        chat_id = "TEST_CHAT_ID"
        limit = 10
        exclusive_start_key = None

        # Mocking the ClientError exception from DynamoDB query
        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )
        
        # Call the method under test 
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_chat_messages(chat_id, limit, exclusive_start_key)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(chat_id),  
            Limit=limit  
        )
        self.assertEqual(e.exception.status_code, 400)



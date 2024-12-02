import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dataclasses import asdict

from repository import ChatRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from utils import Singleton
from model import ChatSession, ChatMessage, Chat, ChatInteraction, ChatContext, ChatCreationDate


class TestChatRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/chatbot/'
    TEST_USER_ID = 'test_user_id'
    TEST_CHAT_ID = 'test_chat_id'
    TEST_OWNER_ID = 'test_owner_id'
    TEST_MODEL_ID = 'test_model_id'
    TEST_TITLE = 'test_title'
    TEST_TIMESTAMP = 12345
    CHAT_MESSAGES_LIMIT = 10

    
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


    def test_get_user_chat_sessions_success_case(self):
        """
        Test case for successfully retrieving user chat sessions.

        Expected result: The method returns a list of user chat sessions.
        """
        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_user_chat_sessions_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        chats = self.chat_repository.get_user_chat_sessions(self.TEST_USER_ID)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(self.TEST_USER_ID)
        )
        self.assertEqual(type(chats), list)
        self.assertEqual(len(chats), len(mock_items))
        self.assertEqual(type(chats[0]), ChatSession)


    def test_get_user_chat_sessions_throws_client_exception(self):
        """
        Test case for handling failure while retrieving user chats due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )
        
        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_user_chat_sessions(self.TEST_USER_ID)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(self.TEST_USER_ID)
        )
        self.assertEqual(e.exception.status_code, 400)


    def test_get_user_chat_returns_emptry_response(self):
        """
        Test case for retrieving user chats when there are no chat sessions.

        Expected result: The method returns an empty list.
        """
        # Mock response from DynamoDB query
        self.mock_dynamodb_table.query.return_value = {'Items': []}

        # Call the method under test
        result = self.chat_repository.get_user_chat_sessions(self.TEST_USER_ID)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(self.TEST_USER_ID)
        )
        self.assertEqual(result, [])


    def test_get_chat_messages_success_case(self):
        """
        Test case for successfully retrieving chat messages.

        Expected result: The method returns a list of messager for chat.
        """
        exclusive_start_key = None

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_chat_messages_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)
        mock_last_evaluated_key = {"key": "value"}

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': mock_last_evaluated_key,
        }

        # Call the method under test
        chat_response = self.chat_repository.get_chat_messages(self.TEST_CHAT_ID, self.CHAT_MESSAGES_LIMIT, exclusive_start_key)

        items = chat_response.messages
        last_evaluated_key = chat_response.last_evaluated_key

        # Assertions
        self.assertEqual(type(items), list)
        self.assertEqual(len(items), len(mock_items))
        self.assertEqual(type(items[0]), ChatMessage)  
        self.assertEqual(last_evaluated_key, mock_last_evaluated_key)

        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(self.TEST_CHAT_ID),
            Limit=self.CHAT_MESSAGES_LIMIT,
            ScanIndexForward=False
        )


    def test_get_chat_messages_with_using_last_evaluated_key(self):
        """
        Test case for successfully retrieving chat messages with last evaluated key and limit.

        Expected result: The method returns a list of messager for chat and a last evaluated key.
        """
        exclusive_start_key = {"some_key": "some_value"}  

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_chat_messages_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)
        mock_last_evaluated_key = {"key": "new_value"}

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': mock_last_evaluated_key
        }

        # Call the method under test
        chat_response = self.chat_repository.get_chat_messages(self.TEST_CHAT_ID, self.CHAT_MESSAGES_LIMIT, exclusive_start_key)

        items = chat_response.messages
        last_evaluated_key = chat_response.last_evaluated_key

        # Assertions
        self.assertEqual(type(items), list)
        self.assertEqual(len(items), len(mock_items))
        self.assertEqual(type(items[0]), ChatMessage)
        self.assertEqual(last_evaluated_key, mock_last_evaluated_key)

        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(self.TEST_CHAT_ID),
            Limit=self.CHAT_MESSAGES_LIMIT,
            ExclusiveStartKey=exclusive_start_key,
            ScanIndexForward=False
        )


    def test_get_chat_messages_without_using_last_evaluated_key(self):
        """
        Test case for successfully retrieving chat messages without last evaluated key.

        Expected result: The method returns a list of messager for chat and a last evaluated key.
        """
        exclusive_start_key = None

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_chat_messages_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        chat_response = self.chat_repository.get_chat_messages(self.TEST_CHAT_ID, self.CHAT_MESSAGES_LIMIT, exclusive_start_key)

        items = chat_response.messages

        # Assertions
        self.assertEqual(type(items), list)
        self.assertEqual(len(items), len(mock_items))
        self.assertIsNone(chat_response.last_evaluated_key)
        self.assertEqual(type(items[0]), ChatMessage)  

        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(self.TEST_CHAT_ID),
            Limit=self.CHAT_MESSAGES_LIMIT,
            ScanIndexForward=False
        )


    def test_get_chat_messages_throws_client_exception(self):
        """
        Test case for handling ClientError while fetching chat message.

        Expected Result: The method raises a ServiceException.
        """
        exclusive_start_key = None

        # Mocking the ClientError exception from DynamoDB query
        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )
        
        # Call the method under test 
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_chat_messages(self.TEST_CHAT_ID, self.CHAT_MESSAGES_LIMIT, exclusive_start_key)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(self.TEST_CHAT_ID),  
            Limit=self.CHAT_MESSAGES_LIMIT,
            ScanIndexForward=False  
        )
        self.assertEqual(e.exception.status_code, 400)


    def test_get_chat_messages_returns_empty_response(self):
        """
        Test case for retrieving chat messages when there are no messages in chat.

        Expected result: The method returns an empty list.
        """
        # Mock response from DynamoDB query
        self.mock_dynamodb_table.query.return_value = {
            'Items': [],
            'LastEvaluatedKey': None,
        }
        
        # Call the method under test
        result = self.chat_repository.get_chat_messages(self.TEST_CHAT_ID, self.CHAT_MESSAGES_LIMIT)
        
        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(self.TEST_CHAT_ID),
            Limit=self.CHAT_MESSAGES_LIMIT,
            ScanIndexForward=False
        )
        self.assertEqual(result.messages, [])
        self.assertEqual(result.last_evaluated_key, None)


    def test_create_new_chat_success(self):
        """
        Test case for successfully inserting an item into the DynamoDB table.

        Expected result: The method should successfully add a new chat item to DynamoDB.
        """
        item = Chat(
            user_id= self.TEST_USER_ID,
            owner_id= self.TEST_OWNER_ID,
            model_id= self.TEST_MODEL_ID,
        )
        
        # Mock response from DynamoDB put_item
        self.mock_dynamodb_table.put_item.return_value = {}

        # Call the method under test
        self.chat_repository.create_new_chat(item)

        # Assertions
        self.mock_dynamodb_table.put_item.assert_called_once_with(Item=asdict(item))
        

    def test_create_new_chat_throws_client_exception(self):
        """
        Test case for handling ClientError while creating an item into the DynamoDB table.

        Expected result: The method should raise a ServiceException if DynamoDB put_item fails.
        """
        item = Chat(
            user_id= self.TEST_USER_ID,
            owner_id= self.TEST_OWNER_ID,
            model_id= self.TEST_MODEL_ID,
        )
        
        # Mocking the ClientError exception from DynamoDB put_item
        self.mock_dynamodb_table.put_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'put_item'
        )
        
        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.create_new_chat(item)

        # Assertions
        self.mock_dynamodb_table.put_item.assert_called_once_with(Item=asdict(item))
        self.assertEqual(e.exception.status_code, 400)


    def test_save_chat_interaction_success(self):
        """
        Test case for successfully inserting an item into the DynamoDB table.

        Expected result: The method should successfully add a new chat item to DynamoDB.
        """
        chat_interaction = ChatInteraction(
            chat_id= self.TEST_CHAT_ID,
            prompt= 'prompt message',
            response= 'message response',
        )

         # Mock response from DynamoDB put_item
        self.mock_dynamodb_table.put_item.return_value = {}

         # Call the method under test
        self.chat_repository.save_chat_interaction(chat_interaction)

        # Assertions
        self.mock_dynamodb_table.put_item.assert_called_once_with(Item=asdict(chat_interaction))


    def test_save_chat_interaction_throws_client_exception(self):
        """
        Test case for handling ClientError while creating an item into the DynamoDB table.

        Expected result: The method should raise a ServiceException if DynamoDB put_item fails.
        """
        item = ChatInteraction(
            chat_id= self.TEST_CHAT_ID,
            prompt= 'prompt message',
            response= 'message response',
        )
        
        # Mocking the ClientError exception from DynamoDB put_item
        self.mock_dynamodb_table.put_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'put_item'
        )
        
        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.save_chat_interaction(item)

        # Assertions
        self.mock_dynamodb_table.put_item.assert_called_once_with(Item=asdict(item))
        self.assertEqual(e.exception.status_code, 400)


    def test_update_title_in_chat_context_success(self):
        """
        Test case for successfully updating item in DynamoDB.
        """
        updated_item = {
            'model_id': self.TEST_MODEL_ID,
            'title': self.TEST_TITLE,
        }
        
        # Mock successful update
        self.mock_dynamodb_table.update_item.return_value = {"Attributes": updated_item}

        # Call the method under test
        result = self.chat_repository.update_title_in_chat_context(chat_id=self.TEST_CHAT_ID, timestamp=self.TEST_TIMESTAMP, title=self.TEST_TITLE)

        # Assertions
        self.mock_dynamodb_table.update_item.assert_called_once_with(
            Key={'chat_id': self.TEST_CHAT_ID, 'timestamp': self.TEST_TIMESTAMP},
            UpdateExpression="SET title = :title",
            ExpressionAttributeValues={":title": self.TEST_TITLE},
            ReturnValues="ALL_NEW"                           
        )
        self.assertIsInstance(result, ChatContext)
        self.assertEqual(result.model_id, self.TEST_MODEL_ID)
        self.assertEqual(result.title, self.TEST_TITLE)


    def test_update_title_in_chat_context_throws_client_exception(self):
        """
        Test case for handling DynamoDB ClientError when updating item.
        """
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.update_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            'update_item'
        )

        # Test exception handling
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.update_title_in_chat_context(chat_id=self.TEST_CHAT_ID, timestamp=self.TEST_TIMESTAMP, title=self.TEST_TITLE)

        self.assertEqual(e.exception.status_code, 400)
        self.mock_dynamodb_table.update_item.assert_called_once()


    def test_get_chat_context_success(self):
        """
        Test case for successfully retrieving chat context from DynamoDB.
        """
        # Mock DynamoDB response
        item = TestUtils.get_file_content(self.test_resource_path + 'get_chat_context_response.json')
        self.mock_dynamodb_table.get_item.return_value = {"Item": item} 

        # Call method
        chat_context = self.chat_repository.get_chat_context(chat_id=self.TEST_CHAT_ID, timestamp=self.TEST_TIMESTAMP)

        # Assertions
        self.assertIsInstance(chat_context, ChatContext)
        self.assertEqual(chat_context.model_id, 'model123')
        self.assertEqual(chat_context.title, 'chat_title')
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={'chat_id': self.TEST_CHAT_ID, 'timestamp': self.TEST_TIMESTAMP})


    def test_get_chat_context_throws_client_exception(self):
        """
        Test case for handling DynamoDB ClientError when retrieving chat context.
        """
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.get_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            'get_item'
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_chat_context(chat_id=self.TEST_CHAT_ID, timestamp=self.TEST_TIMESTAMP)

        # Assertion    
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Failed to retrieve chat context")
        self.mock_dynamodb_table.get_item.assert_called_once()


    def test_get_chat_context_returns_empty_response(self):
        """
        Test case for retrieving chats context when there are no chat available.

        Expected Result: The method raises a ServiceException indicating that chat context does not exist.
        """
        # Mock response from Dynamodb get_item
        self.mock_dynamodb_table.get_item.return_value = {"Item": {}}

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_chat_context(chat_id=self.TEST_CHAT_ID, timestamp=self.TEST_TIMESTAMP)

        # Assertion
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Chat context does not exists")


    def test_get_timestamp(self):
        """
        Test case for successfully retrieving the timestamp of a chat from DynamoDB.

        Expected result: The method returns timestamp for chat.
        """
        # Mock DynamoDB response
        mock_table_items_path = self.test_resource_path + "get_timestamp_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value ={
            'Items': mock_items
        }

        # Call method
        chat_timestamp = self.chat_repository.get_chat_timestamp(user_id=self.TEST_USER_ID, chat_id=self.TEST_CHAT_ID)

        # Assertions
        self.assertIsInstance(chat_timestamp, ChatCreationDate)
        self.assertEqual(chat_timestamp.timestamp, self.TEST_TIMESTAMP)
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(self.TEST_USER_ID) & Key('chat_id').eq(self.TEST_CHAT_ID)
        )

    
    def test_get_timestamp_throws_client_exception(self):
        """
         Test case for handling DynamoDB ClientError when retrieving the chat timestamp.

         Expected Result: The method raises a ServiceException.
        """
        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_chat_timestamp(user_id=self.TEST_USER_ID, chat_id=self.TEST_CHAT_ID)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,  
            KeyConditionExpression=Key('user_id').eq(self.TEST_USER_ID) & Key('chat_id').eq(self.TEST_CHAT_ID)
        )
        self.assertEqual(e.exception.status_code, 400)


    def test_get_timestamp_returns_empty_response(self):
        """
        Test case for retrieving chat timestamp when there are no chat available.

        Expected result: The method returns None.
        """
        # Mock response from DynamoDB query
        self.mock_dynamodb_table.query.return_value = {'Items': []}

        # Call the method under test
        result = self.chat_repository.get_chat_timestamp(user_id=self.TEST_USER_ID, chat_id=self.TEST_CHAT_ID)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(self.TEST_USER_ID) & Key('chat_id').eq(self.TEST_CHAT_ID)
        )
        self.assertIsNone(result)

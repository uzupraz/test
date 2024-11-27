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
        user_id = "TEST_USER_ID"

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_user_chat_sessions_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        chats = self.chat_repository.get_user_chat_sessions(user_id)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        self.assertEqual(type(chats), list)
        self.assertEqual(len(chats), len(mock_items))
        self.assertEqual(type(chats[0]), ChatSession)


    def test_get_user_chat_sessions_throws_client_exception(self):
        """
        Test case for handling failure while retrieving user chats due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
        user_id = "TEST_USER_ID"

        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )
        
        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_user_chat_sessions(user_id)

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        self.assertEqual(e.exception.status_code, 400)


    def test_get_chat_messages_success_case(self):
        """
        Test case for successfully retrieving chat messages.

        Expected result: The method returns a list of messager for chat.
        """
        chat_id = "TEST_CHAT_ID"
        limit = 10
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
            Limit=limit,
            ScanIndexForward=False
        )


    def test_get_chat_messages_with_using_last_evaluated_key(self):
        """
        Test case for successfully retrieving chat messages with last evaluated key and limit.

        Expected result: The method returns a list of messager for chat and a last evaluated key.
        """
        chat_id = "TEST_CHAT_ID"
        limit = 10
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
            Limit=limit,
            ExclusiveStartKey=exclusive_start_key,
            ScanIndexForward=False
        )



    def test_get_chat_messages_without_using_last_evaluated_key(self):
        """
        Test case for successfully retrieving chat messages without last evaluated key.

        Expected result: The method returns a list of messager for chat and a last evaluated key.
        """
        chat_id = "TEST_CHAT_ID"
        limit = 10
        exclusive_start_key = None

        # Mock response from DynamoDB query
        mock_table_items_path = self.test_resource_path + "get_chat_messages_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value = {
            'Items': mock_items,
        }

        # Call the method under test
        chat_response = self.chat_repository.get_chat_messages(chat_id, limit, exclusive_start_key)

        items = chat_response.messages

        # Assertions
        self.assertEqual(type(items), list)
        self.assertEqual(len(items), len(mock_items))
        self.assertEqual(type(items[0]), ChatMessage)  

        self.mock_dynamodb_table.query.assert_called_once_with(
            KeyConditionExpression=Key('chat_id').eq(chat_id),
            Limit=limit,
            ScanIndexForward=False
        )


    def test_get_chat_messages_throws_client_exception(self):
        """
        Test case for handling ClientError while fetching chat message.

        Expected Result: The method raises a ServiceException.
        """
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
            Limit=limit,
            ScanIndexForward=False  
        )
        self.assertEqual(e.exception.status_code, 400)


    def test_create_new_chat_success(self):
        """
        Test case for successfully inserting an item into the DynamoDB table.

        Expected result: The method should successfully add a new chat item to DynamoDB.
        """
        item =Chat(
            user_id= 'test_user_id',
            owner_id= 'test_owner_id',
            model_id= 'test_model_id',
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
            user_id= 'test_user_id',
            owner_id= 'test_owner_id',
            model_id= 'test_model_id',
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
            chat_id= 'test_chat_id',
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
            chat_id= 'test_chat_id',
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
        chat_id = 'chat123'
        timestamp = 12345
        title = 'test title'
        model_id = 'test_model_id'
        updated_item = {
            'model_id': 'test_model_id',
            'title': 'test title',
        }
        
        # Mock successful update
        self.mock_dynamodb_table.update_item.return_value = {"Attributes": updated_item}

        # Call the method under test
        result = self.chat_repository.update_title_in_chat_context(chat_id=chat_id, timestamp=timestamp, title=title, return_updated_item=True)

        # Assertions
        self.mock_dynamodb_table.update_item.assert_called_once_with(
            Key={'chat_id': chat_id, 'timestamp': timestamp},
            UpdateExpression="SET title = :title",
            ExpressionAttributeValues={":title": title},
            ReturnValues="ALL_NEW"                           
        )
        self.assertIsInstance(result, ChatContext)
        self.mock_dynamodb_table.update_item.assert_called_once()
        self.assertEqual(result.model_id, model_id)
        self.assertEqual(result.title, title)


    def test_update_title_in_chat_context_return_value_false(self):
        """
        Test case for successfully updating item in DynamoDB.
        """
        chat_id = 'chat123'
        timestamp = 12345
        title = 'test title'
        # Mock successful update
        self.mock_dynamodb_table.update_item.return_value = {}

        # Call the method under test
        self.chat_repository.update_title_in_chat_context(chat_id=chat_id, timestamp=timestamp, title=title, return_updated_item=False)

        # Assertions
        self.mock_dynamodb_table.update_item.assert_called_once_with(
            Key={'chat_id': chat_id, 'timestamp': timestamp},
            UpdateExpression="SET title = :title",
            ExpressionAttributeValues={":title": title},
            ReturnValues="NONE"
        )
        self.mock_dynamodb_table.update_item.assert_called_once()


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
            self.chat_repository.update_title_in_chat_context(chat_id='chat123', timestamp=12345, title='chat_title', return_updated_item=False)

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
        chat_context = self.chat_repository.get_chat_context(chat_id='chat123', timestamp=12345)

        # Assertions
        self.assertIsInstance(chat_context, ChatContext)
        self.assertEqual(chat_context.model_id, 'model123')
        self.assertEqual(chat_context.title, 'chat_title')
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={'chat_id': 'chat123', 'timestamp': 12345})


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
            self.chat_repository.get_chat_context(chat_id='chat123', timestamp=12345)

        # Assertion    
        self.assertEqual(e.exception.status_code, 400)
        self.mock_dynamodb_table.get_item.assert_called_once()


    def test_get_timestamp_success_case(self):
        """
        Test case for successfully retrieving the timestamp of a chat from DynamoDB.
        """
        user_id = 'user123'
        chat_id = 'chat123'

        # Mock DynamoDB response
        mock_table_items_path = self.test_resource_path + "get_timestamp_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_dynamodb_table.query.return_value ={
            'Items': mock_items
        }

        # Call method
        chat_timestamp = self.chat_repository.get_chat_timestamp(user_id=user_id, chat_id=chat_id)

        # Assertions
        self.assertIsInstance(chat_timestamp, ChatCreationDate)
        self.assertEqual(chat_timestamp.timestamp, 12345)
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,
            KeyConditionExpression=Key('user_id').eq(user_id) & Key('chat_id').eq(chat_id)
        )

    
    def test_get_timestamp_throws_client_exception(self):
        """
         Test case for handling DynamoDB ClientError when retrieving the chat timestamp.
        """
        self.mock_dynamodb_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query'
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_chat_timestamp(user_id='user123', chat_id='chat123')

        # Assertions
        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.app_config.chatbot_messages_gsi_name,  
            KeyConditionExpression=Key('user_id').eq('user123') & Key('chat_id').eq('chat123')
        )
        self.assertEqual(e.exception.status_code, 400)

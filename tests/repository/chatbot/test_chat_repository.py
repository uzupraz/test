import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dataclasses import asdict

from repository import ChatRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from utils import Singleton
from model import ChatSession, ChatMessage, Chat, ChildChat, ParentInfo, ChatCreationDate


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


    def test_get_chat_messages_success_case(self):
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
            Limit=limit,
        )


    def test_get_chat_messages_with_using_last_evaluated_key(self):
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
            ExclusiveStartKey=exclusive_start_key
        )



    def test_get_chat_messages_without_using_last_evaluated_key(self):
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
            KeyConditionExpression=Key('chat_id').eq(chat_id),Limit=limit
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


    def test_create_new_chat_success(self):
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


    def test_save_message_success(self):
        item = ChildChat(
            chat_id= 'test_chat_id',
            prompt= 'prompt message',
            response= 'message response',
        )

         # Mock response from DynamoDB put_item
        self.mock_dynamodb_table.put_item.return_value = {}

         # Call the method under test
        self.chat_repository.save_message(item)

        # Assertions
        self.mock_dynamodb_table.put_item.assert_called_once_with(Item=asdict(item))


    def test_save_message_throws_client_exception(self):
        item = ChildChat(
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
            self.chat_repository.save_message(item)

        # Assertions
        self.mock_dynamodb_table.put_item.assert_called_once_with(Item=asdict(item))
        self.assertEqual(e.exception.status_code, 400)


    def test_update_parent_chat_title_success(self):
        chat_id = 'chat123'
        timestamp = 12345
        title = 'test title'
        # Mock successful update
        self.mock_dynamodb_table.update_item.return_value = {}

        # Call the method under test
        self.chat_repository.update_parent_chat_title(chat_id=chat_id, timestamp=timestamp, title=title)

        # Assertions
        self.mock_dynamodb_table.update_item.assert_called_once_with(
            Key={'chat_id': chat_id, 'timestamp': timestamp},
            UpdateExpression="SET #title = if_not_exists(#title, :title)",
            ExpressionAttributeNames={"#title": "title"},
            ExpressionAttributeValues={":title": title}
        )
        self.mock_dynamodb_table.update_item.assert_called_once()


    def test_update_parent_chat_title_throws_client_exception(self):
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.update_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            'update_item'
        )

        # Test exception handling
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.update_parent_chat_title(chat_id='chat123', timestamp=12345, title='chat_title')

        self.assertEqual(e.exception.status_code, 400)
        self.mock_dynamodb_table.update_item.assert_called_once()


    def test_get_parent_info_success(self):
        # Mock DynamoDB response
        item = TestUtils.get_file_content(self.test_resource_path + 'get_parent_info_response.json')
        self.mock_dynamodb_table.get_item.return_value = {"Item": item} 

        # Call method
        parent_info = self.chat_repository.get_parent_info(chat_id='chat123', timestamp=12345)

        # Assertions
        self.assertIsInstance(parent_info, ParentInfo)
        self.assertEqual(parent_info.chat_id, 'chat123')
        self.assertEqual(parent_info.model_id, 'model123')
        self.assertEqual(parent_info.title, 'chat_title')
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={'chat_id': 'chat123', 'timestamp': 12345})


    def test_get_parent_info_throws_client_exception(self):
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.get_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            'get_item'
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.chat_repository.get_parent_info(chat_id='chat123', timestamp=12345)

        # Assertion    
        self.assertEqual(e.exception.status_code, 400)
        self.mock_dynamodb_table.get_item.assert_called_once()


    def test_get_timestamp_success_case(self):
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

import json
import unittest
from unittest.mock import MagicMock, patch
from dacite import from_dict

from tests.test_utils import TestUtils
from exception import ServiceException
from model import Chat, ChatMessage, SaveChatResponse, ChatResponse, MessageHistoryResponse, ChatInteraction, ChatContext, ChatSession
from service import ChatService
from enums import ServiceStatus


class TestChatService(unittest.TestCase):

    
    TEST_RESOURCE_PATH = '/tests/resources/chatbot/'
    TEST_USER_ID = 'test_user_id'
    TEST_CHAT_ID = 'test_chat_id'
    TEST_OWNER_ID = 'test_owner_id'
    TEST_MODEL_ID = 'test_model_id'
    TEST_TIMESTAMP = 12345
    ENCODING_FORMAT = 'utf-8'

    
    def setUp(self) -> None:
        self.mock_chat_repository = MagicMock()
        self.mock_bedrock_service = MagicMock()
        self.chat_service = ChatService(self.mock_chat_repository, self.mock_bedrock_service)


    def tearDown(self) -> None:
        self.mock_chat_repository = None
        self.chat_service = None


    def test_get_chats_success_case(self):

        """
        Test case for successfully retrieving chats.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + 'get_user_chat_sessions_response.json'
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        mock_chats = [
            from_dict(ChatSession, mock_items[0]),
            from_dict(ChatSession, mock_items[1])
        ]

        self.chat_service.chat_repository.get_user_chat_sessions = MagicMock(return_value=mock_chats)

        mock_chat_context = from_dict(ChatContext, {
            'model_id': 'test_model',
            'title': 'ChatTitle'
        })

        self.chat_service.chat_repository.get_chat_context = MagicMock(return_value=mock_chat_context)

        result = self.chat_service.get_chats(self.TEST_USER_ID)
        expected_result = [
            ChatResponse(chat_id="TEST_CHAT_ID", created_at=12345, title="ChatTitle"),
            ChatResponse(chat_id="TEST_CHAT_ID2", created_at=67890, title="ChatTitle"),
        ]
        self.assertEqual(result, expected_result)


    def test_get_chats_empty_case(self):
        """
        Test case for retrieving empty chats.
        """
        self.chat_service.chat_repository.get_user_chat_sessions = MagicMock(return_value=[])

        result = self.chat_service.get_chats(self.TEST_USER_ID)

        self.assertEqual(result, [])


    def test_get_chats_failure(self):
        """
        Test case for handling failure in the repository layer.
        Expected Result: ServiceException is raised by the service layer.
        """
        self.chat_service.chat_repository.get_user_chat_sessions = MagicMock()
        self.chat_service.chat_repository.get_user_chat_sessions.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to retrieve chats'
        )

        with self.assertRaises(ServiceException) as context:
            self.chat_service.get_chats(self.TEST_USER_ID)

        self.assertEqual(context.exception.message, 'Failed to retrieve chats')
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)


    def test_get_messages_history_success_case(self):
        """
        Test case for successfully retrieving messages history with pagination.
        """
        size = 3
        last_evaluated_key = None

        mock_table_items_path = self.TEST_RESOURCE_PATH + 'get_chat_messages_response.json'
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        mock_chat_messages = [
            from_dict(ChatMessage, mock_items[0]),
            from_dict(ChatMessage, mock_items[1]),
            from_dict(ChatMessage, mock_items[2])
        ]
        
        mock_repo_response = MagicMock()
        mock_repo_response.messages = mock_chat_messages
        mock_repo_response.last_evaluated_key = last_evaluated_key
        
        # Setup mock repository
        self.chat_service.chat_repository.get_chat_messages = MagicMock(
            return_value=mock_repo_response
        )

        result = self.chat_service.get_message_history(
            chat_id=self.TEST_CHAT_ID,
            size=3,
            last_evaluated_key=last_evaluated_key
        )

        self.chat_service.chat_repository.get_chat_messages.assert_called_with(
            chat_id=self.TEST_CHAT_ID,
            limit=3,
            exclusive_start_key=last_evaluated_key
        )

        self.assertIsInstance(result, MessageHistoryResponse)
        self.assertEqual(len(result.messages), size)
        self.assertEqual(result.messages, mock_chat_messages)
        
        self.assertEqual(result.pagination.size, size)

        self.chat_service.chat_repository.get_chat_messages.assert_called_with(
            chat_id=self.TEST_CHAT_ID,
            limit=size,
            exclusive_start_key=last_evaluated_key
        )


    def test_get_messages_history_with_last_evaluated_key(self):
        """
        Test case for successfully retrieving messages history with pagination using a last evaluated key.
        """
        size = 2
        last_evaluated_key = 'eyJzb21lX2tleSI6ICJzb21lX3ZhbHVlIn0='  

        mock_table_items_path = self.TEST_RESOURCE_PATH + 'get_chat_messages_response.json'
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        # Mock chat messages
        mock_chat_messages = [
            from_dict(ChatMessage, mock_items[0]),
            from_dict(ChatMessage, mock_items[1])
        ]

        # Mock repository response
        mock_repo_response = MagicMock()
        mock_repo_response.messages = mock_chat_messages
        mock_repo_response.last_evaluated_key = {"next_key": "next_value"}

        # Mocking repository method
        self.chat_service.chat_repository.get_chat_messages = MagicMock(return_value=mock_repo_response)

        # Call the method under test
        result = self.chat_service.get_message_history(
            chat_id=self.TEST_CHAT_ID,
            size=size,
            last_evaluated_key=last_evaluated_key
        )

        # Assertions for repository method call
        self.chat_service.chat_repository.get_chat_messages.assert_called_with(
            chat_id=self.TEST_CHAT_ID,
            limit=size,
            exclusive_start_key=json.loads(TestUtils.decode_base64(last_evaluated_key))
        )

        # Assertions for the response
        self.assertIsInstance(result, MessageHistoryResponse)
        self.assertEqual(len(result.messages), size)
        self.assertEqual(result.messages, mock_chat_messages)

        # Validate pagination in the result
        self.assertIsNotNone(result.pagination)
        self.assertEqual(result.pagination.size, size)
        self.assertEqual(result.pagination.last_evaluated_key, TestUtils.encode_to_base64(json.dumps({"next_key": "next_value"})))


    def test_get_messages_history_failure(self):
        """
        Test case for handling failure in the repository layer.
        Expected Result: ServiceException is raised by the service layer.
        """
        size = 3
        last_evaluated_key = None
        self.chat_service.chat_repository.get_chat_messages = MagicMock()
        self.chat_service.chat_repository.get_chat_messages.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to retrieve messages'
        )

        with self.assertRaises(ServiceException) as context:
            self.chat_service.get_message_history(self.TEST_CHAT_ID, size, last_evaluated_key)

        self.assertEqual(context.exception.message, 'Failed to retrieve messages')
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)


    def test_save_chat_session_success_case(self):
        """
        Test case for saving a new chat session.
        """
        # Mock the repository method
        self.chat_service.chat_repository.create_new_chat = MagicMock()

        chat_id = self.TEST_CHAT_ID

        with patch('nanoid.generate', return_value=chat_id):
            expected_chat = Chat(
                user_id=self.TEST_USER_ID,
                owner_id=self.TEST_OWNER_ID,
                model_id=self.TEST_MODEL_ID,
            )
            expected_chat.chat_id = chat_id  

            expected_response = SaveChatResponse(chat_id=chat_id)

            # Call the method under test
            result = self.chat_service.save_chat_session(
                user_id=self.TEST_USER_ID,
                owner_id=self.TEST_OWNER_ID,
                model_id=self.TEST_MODEL_ID,
            )

            # Assertions for the response
            self.assertIsInstance(result, SaveChatResponse)
            self.assertEqual(result.chat_id, expected_response.chat_id)

            self.chat_service.chat_repository.create_new_chat.assert_called_with(item=expected_chat) 


    def test_save_chat_session_should_raise_exception_when_repository_call_fails(self): 
        """
        Test case for handling failure in the repository layer.
        Expected Result: ServiceException is raised.
        """
        chat_id = self.TEST_CHAT_ID
        mock_create_new_chat = self.chat_service.chat_repository.create_new_chat = MagicMock()
        mock_create_new_chat.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to create chat session'
        )

        with patch('nanoid.generate', return_value=chat_id):
            expected_chat = Chat(
                user_id=self.TEST_USER_ID,
                owner_id=self.TEST_OWNER_ID,
                model_id=self.TEST_MODEL_ID,
            )
            expected_chat.chat_id = chat_id  

            with self.assertRaises(ServiceException) as context:
                self.chat_service.save_chat_session(self.TEST_USER_ID, self.TEST_OWNER_ID, self.TEST_MODEL_ID)

            self.assertEqual(context.exception.message, 'Failed to create chat session')
            self.assertEqual(context.exception.status_code, 500)

            self.chat_service.chat_repository.create_new_chat.assert_called_once_with(item=expected_chat)

    
    def test_save_chat_interaction_success_case(self):
        """
        Test case for successfully saving a chat interaction and streaming the response.
        """
        # Test data
        test_prompt = "Hello, how are you?"
        test_response_chunks = ["Hello", ", I'm", " doing well!"]
        test_full_response = "Hello, I'm doing well!"
        test_timestamp = 12345

        # Mock chat timestamp response
        mock_timestamp_response = MagicMock()
        mock_timestamp_response.timestamp = test_timestamp
        
        # Mock cjat context
        mock_chat_context = from_dict(ChatContext, {
            'model_id': self.TEST_MODEL_ID,
            'title': 'Test Chat'
        })

        # Setup repository mocks
        self.chat_service.chat_repository.get_chat_timestamp = MagicMock(
            return_value=mock_timestamp_response
        )
        self.chat_service.chat_repository.get_chat_context = MagicMock(
            return_value=mock_chat_context
        )
        self.chat_service.chat_repository.save_chat_interaction = MagicMock()

        # Mock _get_chat_interaction_records
        self.chat_service._get_chat_interaction_records = MagicMock(return_value=[])

        # Mock bedrock service response
        self.chat_service.bedrock_service.send_prompt_to_model = MagicMock(
            return_value=iter(test_response_chunks)
        )

        # Call the method and collect streamed responses
        response_chunks = []
        for chunk in self.chat_service.save_chat_interaction(
            user_id=self.TEST_USER_ID,
            chat_id=self.TEST_CHAT_ID,
            prompt=test_prompt
        ):
            response_chunks.append(chunk)

        # Verify the responses were streamed correctly
        self.assertEqual(response_chunks, test_response_chunks)

        # Verify repository calls
        self.chat_service.chat_repository.get_chat_timestamp.assert_called_once_with(
            self.TEST_USER_ID, 
            self.TEST_CHAT_ID
        )
        self.chat_service.chat_repository.get_chat_context.assert_called_once_with(
            self.TEST_CHAT_ID, 
            test_timestamp
        )

        # Verify message was saved with complete response
        expected_chat_info = ChatInteraction(
            chat_id=self.TEST_CHAT_ID,
            prompt=test_prompt,
            response=test_full_response
        )
        self.chat_service.chat_repository.save_chat_interaction.assert_called_once_with(
            chat_interaction=expected_chat_info
        )


    def test_save_chat_interaction_failure(self):
        """
        Test case for handling failure when saving a chat interaction.
        Expected Result: ServiceException is raised.
        """
        test_prompt = "Hello, how are you?"
        
        # Mock chat timestamp response
        mock_timestamp_response = MagicMock()
        mock_timestamp_response.timestamp = self.TEST_TIMESTAMP

        # Mock chat context
        mock_chat_context = from_dict(ChatContext, {
            'model_id': self.TEST_MODEL_ID,
            'title': 'Test Chat'
        })

        # Setup repository mocks
        self.chat_service.chat_repository.get_chat_timestamp = MagicMock(
            return_value=mock_timestamp_response
        )
        self.chat_service.chat_repository.get_chat_context = MagicMock(
            return_value=mock_chat_context
        )

        # Mock bedrock service to raise an exception
        self.chat_service.bedrock_service.send_prompt_to_model = MagicMock(
            side_effect=Exception("Failed to stream response")
        )

        # Mock _get_chat_interaction_records
        self.chat_service._get_chat_interaction_records = MagicMock(return_value=[])

        # Verify the exception is raised
        with self.assertRaises(ServiceException) as context:
            list(self.chat_service.save_chat_interaction(
                user_id=self.TEST_USER_ID,
                chat_id=self.TEST_CHAT_ID,
                prompt=test_prompt
            ))

        self.assertEqual(context.exception.message, 'Failed to save chat interaction.')
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
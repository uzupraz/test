from dacite import from_dict
from typing import List

from controller import common_controller as common_ctrl
from repository import ChatRepository
from exception import ServiceException
from enums import ServiceStatus
from model import Chat, ChatMessage, SaveChatResponse, ChatResponse, MessageHistoryResponse, MessageHistoryPagination, ChildChat, Message
from utils import Singleton, Base64ConversionUtils
from service.bedrock_service import BedrockService

log = common_ctrl.log
ENCODING_FORMAT = 'utf-8'


class ChatService(metaclass=Singleton):


    def __init__(self, chat_repository: ChatRepository, bedrock_service: BedrockService) -> None:
        """
        Initializes the ChatService with a ChatRepository instance.

        Args:
            chat_repository (ChatRepository): Repository instance for accessing chat data.
        """
        self.chat_repository = chat_repository
        self.bedrock_service = bedrock_service

    
    def get_chats(self, user_id) -> List[ChatResponse]:
        """
        Retrieves the list of chat sessions for a given user.

        Args:
            user_id (str): The ID of the user whose chats are to be retrieved.

        Returns:
            List[ChatResponse]: A list of ChatResponse objects containing the chat ID, title,
                                and creation timestamp for each chat.
        """
        log.info("Retriving chats for user %s", user_id)

        response = self.chat_repository.get_user_chats(user_id)
        chat_response = []
        for chat in response:
            created_at = chat.timestamp
            chat_response.append(from_dict(ChatResponse, {'chat_id': chat.chat_id, 'created_at': created_at, 'title': self._get_chat_title(chat.chat_id, chat.timestamp)}))

        return chat_response
    

    def get_message_history(self, chat_id: str, size: int, last_evaluated_key: str = None) -> MessageHistoryResponse:
        """
        Retrieves a paginated message history for a given chat session.

        Args:
            chat_id (str): The ID of the chat session.
            size (int): The number of messages to retrieve per page.
            last_evaluated_key (str, optional): Encoded string for pagination, indicating the last evaluated key.

        Returns:
            MessageHistoryResponse: An object containing the list of Message objects and pagination information.
        """
        log.info('Retrieving message history for chat. chat_id: %s', chat_id)
        
        # Decode the last evaluated key if provided
        if last_evaluated_key:
            last_evaluated_key = Base64ConversionUtils.decode_to_dict(last_evaluated_key, ENCODING_FORMAT)

        # Fetch a page of messages from the repository
        chat_messages_response= self.chat_repository.get_chat_messages(
            chat_id=chat_id,
            limit=size,
            exclusive_start_key=last_evaluated_key
        )
        last_evaluated_key = chat_messages_response.last_evaluated_key

        # Convert each retrieved item to a ChatMessage object if it has both a prompt and response
        chat_messages = [ChatMessage(prompt=item.prompt, response=item.response, timestamp=item.timestamp) for item in chat_messages_response.messages if item.prompt is not None and item.response is not None]

        # Encode the last evaluated key for the next client request
        encoded_last_evaluated_key = None
        if last_evaluated_key:
            encoded_last_evaluated_key = Base64ConversionUtils.encode_dict(last_evaluated_key, ENCODING_FORMAT)

        # Return a MessageHistoryResponse with messages and pagination info
        return MessageHistoryResponse(
            messages=chat_messages,
            pagination=MessageHistoryPagination(
                size=size,
                last_evaluated_key=encoded_last_evaluated_key
            )
        )
    

    def save_chat_session(self, user_id: str, owner_id: str, model_id: str) -> SaveChatResponse:
        """
        Creates a new chat session for a user and stores it in the repository.

        Args:
            user_id (str): The ID of the user who is creating the chat.
            owner_id (str): The ID of the owner associated with this chat session.
            model: The name of the model associated with this chat session.

        Returns:
            SaveChatSessionResponse: A model object containing the new chat ID.
        """
        chat = Chat(
            user_id= user_id,
            owner_id=owner_id,
            model_id=model_id
        )
        self.chat_repository.create_new_chat(item=chat)

        return SaveChatResponse(chat_id=chat.chat_id)

    
    def save_chat_message(self, user_id, chat_id: str, prompt: str):
        """
        Saves a chat message and streams the response.

        This method returns a generator that yields response chunks as they are received.

        Args:
            chat_id (str): The ID of the chat session to save the message for.
            prompt (str): The user-provided prompt that is part of the chat message.

        Yields:
            str: Response chunks from the chat stream, or an error message if an error occurs.
        """
        # Create initial chat object with empty response
        child_chat = ChildChat(
            chat_id=chat_id,
            prompt=prompt,
            response=""
        )
        
        try:
            chat_timestamp = self.chat_repository.get_chat_timestamp(user_id, chat_id)
            parent_info = self.chat_repository.get_parent_info(chat_id, chat_timestamp.timestamp)
            if not parent_info.title:
                # Generate the title if it's missing
                log.info("Title not found for parent chat. Generating new title.")
                title = self.bedrock_service.generate_title(message=prompt)
                self.chat_repository.update_parent_chat_title(chat_id, chat_timestamp.timestamp, title)
            else:
                log.info("Title exists for parent chat. Skipping title generation.")

            # Stream and collect response
            response_chunks = []
            
            for chunk in self._stream_chat_message(chat_id, prompt, chat_timestamp.timestamp):
                if chunk.startswith('Error:'):
                    raise ServiceException(500, ServiceStatus.FAILURE, chunk)
                response_chunks.append(chunk)
                yield chunk
            
            child_chat.response = ''.join(response_chunks)
            self.chat_repository.save_message(item=child_chat)
            
        except Exception:
            log.exception('Failed to save chat message. chat_id: %s', chat_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Could not save chat message')


    def _stream_chat_message(self, chat_id: str, prompt: str, timestamp: int):
        """
        Streams the chat message response from the model.

        Args:
            chat_id (str): The ID of the chat session.
            prompt (str): The user-provided prompt.

        Yields:
            str: Response chunks from the model.
        """
        model_id = self._get_chat_model_id(chat_id=chat_id, timestamp=timestamp)   
        messages = self._get_chat_context(chat_id)
        try:
            for response_part in self.bedrock_service.send_prompt_to_model(model_id=model_id, prompt=prompt, messages=messages):
                if response_part:
                    yield response_part
        except Exception:
            log.exception('Failed to stream chat message. chat_id: %s', chat_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Could not stream chat message')


    def _get_chat_context(self, chat_id: str, size: int = 4) -> List[Message]:
        """
        Retrieves the previous messages for a chat session to form the chat context.

        Args:
            chat_id (str): The ID of the chat session.
            size (int): The number of messages to retrieve for the context (default is 4).

        Returns:
            List[Message]: A list of Message objects representing the chat context.
        """
        log.info('Retrieving previous messages for chat. chat_id: %s', chat_id)
        
        chat_message_response = self.chat_repository.get_chat_messages(
            chat_id=chat_id,
            limit=size,
        )
            
        messages = []
        for chat_message in chat_message_response.messages:
            if chat_message.prompt is not None:
                messages.append(Message(
                    role='user',
                    content=chat_message.prompt
                ))
            if chat_message.response is not None:
                messages.append(Message(
                    role='assistant',
                    content=chat_message.response
                ))
        
        return messages


    def _get_chat_title(self, chat_id: str, timestamp: int) -> str:
        """
        Retrieves the title of a chat session.

        Args:
            chat_id (str): The ID of the chat session.
            timestamp (int): The timestamp to fetch the title for.

        Returns:
            str: The title of the chat session.
        """
        parent_info= self.chat_repository.get_parent_info(chat_id, timestamp)
        return parent_info.title
    

    def _get_chat_model_id(self, chat_id: str, timestamp: int) -> str:
        """
        Retrieves the model ID for a given chat session.

        Args:
            chat_id (str): The ID of the chat session.
            timestamp (int): The timestamp to fetch the model ID for.

        Returns:
            str: The model ID associated with the chat session.
        """
        parent_info= self.chat_repository.get_parent_info(chat_id, timestamp)
        return parent_info.model_id
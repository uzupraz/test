from dacite import from_dict
from typing import List

from controller import common_controller as common_ctrl
from repository import ChatRepository
from exception import ServiceException
from enums import ServiceStatus
from model import Chat, ChatMessage, SaveChatResponse, ChatResponse, MessageHistoryResponse, MessageHistoryPagination, ChildChaInfo, Message, ParentChatInfo
from utils import Singleton, Base64ConversionUtils
from service.bedrock.bedrock_service import BedrockService

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
        log.info("Retriving chats for user. user_id: %s", user_id)

        response = self.chat_repository.get_user_chat_sessions(user_id)
        chat_response = []
        for chat in response:
            created_at = chat.timestamp
            parent_info = self.chat_repository.get_parent_chat_info(chat.chat_id, created_at)
            chat_response.append(from_dict(ChatResponse, {'chat_id': chat.chat_id, 'created_at': created_at, 'title': parent_info.title}))

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
        
        if last_evaluated_key:
            last_evaluated_key = Base64ConversionUtils.decode_to_dict(last_evaluated_key, ENCODING_FORMAT)

        chat_messages_response= self.chat_repository.get_chat_messages(
            chat_id=chat_id,
            limit=size,
            exclusive_start_key=last_evaluated_key
        )

        chat_messages = [ChatMessage(prompt=item.prompt, response=item.response, timestamp=item.timestamp) for item in chat_messages_response.messages if item.prompt is not None and item.response is not None]

        encoded_last_evaluated_key = (
            Base64ConversionUtils.encode_dict(chat_messages_response.last_evaluated_key, ENCODING_FORMAT) 
            if chat_messages_response.last_evaluated_key 
            else None
        )

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

    
    def save_chat_message(self, user_id: str, chat_id: str, prompt: str):
        """
        Saves a chat message and streams the response.

        Args:
            user_id (str): The ID of the user.
            chat_id (str): The ID of the chat session.
            prompt (str): The user-provided prompt.

        Yields:
            str: Response chunks from the chat stream.
        """
        child_chat_info = ChildChaInfo(
            chat_id=chat_id,
            prompt=prompt,
            response=""
        )
        
        try:
            parent_chat_info = self._ensure_chat_title(chat_id, user_id, prompt)

            response_chunks = []
            for chunk in self._stream_chat_message(chat_id, prompt, parent_chat_info):
                response_chunks.append(chunk)
                yield chunk
            
            child_chat_info.response = ''.join(response_chunks)
            self.chat_repository.save_message(chat=child_chat_info)
            
        except Exception:
            log.exception('Failed to save chat message. chat_id: %s', chat_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Failed to save chat message')


    def _stream_chat_message(self, chat_id: str, prompt: str, parent_chat_info: ParentChatInfo):
        """
        Streams the chat message response from the model.

        Args:
            chat_id (str): The ID of the chat session.
            prompt (str): The user-provided prompt.
            parent_chat_info: The parent chat info object.

        Yields:
            str: Response chunks from the model.
        """
        messages = self._get_chat_context(chat_id)
        try:
            # Get response iterator from bedrock service
            response_iterator = self.bedrock_service.send_prompt_to_model(
                model_id=parent_chat_info.model_id,
                prompt=prompt,
                messages=messages
            )
            
            # Store the iterator in a variable and then yield from it
            if response_iterator:
                yield from (chunk for chunk in response_iterator if chunk)
                
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
        for chat_message in reversed(chat_message_response.messages):
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
    

    def _ensure_chat_title(self, chat_id: str, user_id: str, prompt: str) -> ParentChatInfo:
        """
        Ensures that a chat has a title, generating one if missing.

        Args:
            chat_id (str): The ID of the chat session.
            user_id (str): The ID of the user.
            prompt (str): The user's prompt to generate title from if needed.

        Returns:
            ParentChatInfo: The parent chat info with guaranteed title.
        """
        chat_timestamp = self.chat_repository.get_chat_timestamp(user_id, chat_id)
        parent_chat_info = self.chat_repository.get_parent_chat_info(chat_id, chat_timestamp.timestamp)
        
        if not parent_chat_info.title:
            log.info("Title not found for parent chat. Generating new title.")
            title = self.bedrock_service.generate_title(message=prompt)
            self.chat_repository.update_parent_chat_title(chat_id, chat_timestamp.timestamp, title)
            # Refresh parent_chat_info to get the updated title
            parent_chat_info = self.chat_repository.get_parent_chat_info(chat_id, chat_timestamp.timestamp)
        
        return parent_chat_info
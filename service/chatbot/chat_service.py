from dacite import from_dict
from typing import List

from controller import common_controller as common_ctrl
from repository import ChatRepository
from exception import ServiceException
from enums import ServiceStatus
from model import Chat, ChatMessage, SaveChatResponse, ChatResponse, MessageHistoryResponse, MessageHistoryPagination, ChatInteraction, InteractionRecord, ChatContext
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
            chat_context = self.chat_repository.get_chat_context(chat.chat_id, created_at)
            chat_response.append(from_dict(ChatResponse, {'chat_id': chat.chat_id, 'created_at': created_at, 'title': chat_context.title}))

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

    
    def save_chat_interaction(self, user_id: str, chat_id: str, prompt: str):
        """
        Saves a chat interaction and streams the response.

        Args:
            user_id (str): The ID of the user.
            chat_id (str): The ID of the chat session.
            prompt (str): The user-provided prompt.

        Yields:
            str: Response chunks from the chat stream.
        """
        chat_interaction = ChatInteraction(
            chat_id=chat_id,
            prompt=prompt,
            response="" # Empty string provides safe starting point before populating it with chunks from stream.
        )
        
        try:
            chat_context = self._ensure_chat_title(chat_id, user_id, prompt)

            response_chunks = []
            for chunk in self._stream_chat_message(chat_id, prompt, chat_context):
                response_chunks.append(chunk)
                yield chunk
            
            chat_interaction.response = ''.join(response_chunks)
            self.chat_repository.save_chat_interaction(chat_interaction=chat_interaction)
            
        except Exception:
            log.exception('Failed to save chat interaction. chat_id: %s', chat_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Failed to save chat interaction.')


    def _stream_chat_message(self, chat_id: str, prompt: str, chat_context: ChatContext):
        """
        Streams the chat message response from the model.

        Args:
            chat_id (str): The ID of the chat session.
            prompt (str): The user-provided prompt.
            chat_context: The ChatContext object.

        Yields:
            str: Response chunks from the model.
        """
        interraction_records = self._get_chat_interaction_records(chat_id)
        try:
            # Get response iterator from bedrock service
            response_iterator = self.bedrock_service.send_prompt_to_model(
                model_id=chat_context.model_id,
                prompt=prompt,
                interaction_records=interraction_records
            )
            
            # Store the iterator in a variable and then yield from it
            if response_iterator:
                yield from (chunk for chunk in response_iterator if chunk)
                
        except Exception:
            log.exception('Failed to stream chat message. chat_id: %s', chat_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Could not stream chat message')


    def _get_chat_interaction_records(self, chat_id: str, size: int = 4) -> List[InteractionRecord]:
        """
        Retrieves the previous chat interactions for a chat session to form the chat context.

        Args:
            chat_id (str): The ID of the chat session.
            size (int): The number of chat interaction to retrieve for the context (default is 4).

        Returns:
            List[InteractionRecord]: A list of InteractionRecord objects representing the chat context.
        """
        log.info('Retrieving previous interaction record for chat. chat_id: %s', chat_id)
        
        chat_message_response = self.chat_repository.get_chat_messages(
            chat_id=chat_id,
            limit=size,
        )
            
        interaction_record = []
        for chat_message in reversed(chat_message_response.messages):
            if chat_message.prompt is not None:
                interaction_record.append(InteractionRecord(
                    role='user',
                    content=chat_message.prompt
                ))
            if chat_message.response is not None:
                interaction_record.append(InteractionRecord(
                    role='assistant',
                    content=chat_message.response
                ))
        
        return interaction_record
    

    def _ensure_chat_title(self, chat_id: str, user_id: str, prompt: str) -> ChatContext:
        """
        Ensures that a chat has a title, generating one if missing.

        Args:
            chat_id (str): The ID of the chat session.
            user_id (str): The ID of the user.
            prompt (str): The user's prompt to generate title from if needed.

        Returns:
            ChatContext: The ChatContext object.
        """
        chat_timestamp = self.chat_repository.get_chat_timestamp(user_id, chat_id)
        chat_context = self.chat_repository.get_chat_context(chat_id, chat_timestamp.timestamp)
        
        if not chat_context.title:
            log.info("Generating chat title. chat_id: %s, user_id: %s", chat_id, user_id)
            title = self.bedrock_service.generate_title(message=prompt)
            updated_chat_context = self.chat_repository.update_title_in_chat_context(
                chat_id=chat_id, 
                timestamp=chat_timestamp.timestamp, 
                title=title,
                return_updated_item=True
            )

            return updated_chat_context
        
        return chat_context
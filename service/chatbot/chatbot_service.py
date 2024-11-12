import base64
import json
import nanoid

from dacite import from_dict
from typing import List, Dict

from controller import common_controller as common_ctrl
from repository import ChatbotRepository
from model import Chat, Message, ChatResponse, MessageHistoryResponse, MessageHistoryPagination
from utils import Singleton


log = common_ctrl.log
ENCODING_FORMAT = 'utf-8'


class ChatbotService(metaclass=Singleton):


    def __init__(self, chatbot_repository: ChatbotRepository) -> None:
        """
        Initializes the ChatbotService with a ChatbotRepository instance.

        Args:
            chatbot_repository (ChatbotRepository): Repository instance for accessing chat data.
        """
        self.chatbot_repository = chatbot_repository

    
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

        chats = self.chatbot_repository.get_user_chats(user_id)
        chat_response = []
        for chat in chats:
            chat_id = chat.get('chat_id')
            created_at = chat.get('timestamp')
            title = self.chatbot_repository.get_chat_title(chat_id, created_at)

            chat_data = {
                'chat_id': chat_id,
                'title': title,
                'created_at': created_at
            }
            chat_response.append(from_dict(ChatResponse, chat_data))

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
        log.info("Retrieving message history for chat %s", chat_id)

        if size is None or not isinstance(size, int):
            size = 20  
        
        # Decode the last evaluated key if provided
        if last_evaluated_key:
            last_evaluated_key = json.loads(base64.b64decode(last_evaluated_key).decode(ENCODING_FORMAT))

        # Fetch a page of messages from the repository
        messages, last_evaluated_key = self.chatbot_repository.get_chat_messages(
            chat_id=chat_id,
            limit=size,
            exclusive_start_key=last_evaluated_key
        )

        # Convert retrieved items to Message objects
        messages = [Message(prompt=item.get('prompt'), response=item.get('response'), timestamp=item.get('timestamp')) for item in messages if item.get('prompt') is not None and item.get('response') is not None]

        # Encode the last evaluated key for the next client request
        encoded_last_evaluated_key = None
        if last_evaluated_key:
            key = json.dumps(last_evaluated_key).encode(ENCODING_FORMAT)
            encoded_last_evaluated_key = base64.b64encode(key).decode(ENCODING_FORMAT)

        # Return a MessageHistoryResponse with messages and pagination info
        return MessageHistoryResponse(
            messages=messages,
            pagination=MessageHistoryPagination(
                size=size,
                last_evaluated_key=encoded_last_evaluated_key
            )
        )
    

    def save_chat_session(self, user_id: str, owner_id: str) -> Dict[str, str]:
        """
        Creates a new chat session for a user and stores it in the repository.

        Args:
            user_id (str): The ID of the user who is creating the chat.
            owner_id (str): The ID of the owner associated with this chat session.

        Returns:
            Dict[str, str]: A dictionary containing the new chat ID.
        """
        chat = Chat(
            chat_id = nanoid.generate(),
            user_id= user_id,
            owner_id=owner_id,
        )
        self.chatbot_repository.create_new_chat(item=chat)
        return {'chat_id': chat.chat_id}
    
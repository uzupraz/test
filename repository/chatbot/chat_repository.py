import boto3 
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dataclasses import asdict
from dacite import from_dict
from typing import List, Optional

from model import Chat, ChatMessage, ChatSession, ChatContext, ChatMessageResponse, ChatInteraction, ChatCreationDate
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton, DataTypeUtils

log = common_ctrl.log


class ChatRepository(metaclass=Singleton):


    def __init__(self, app_config: AppConfig, aws_config: AWSConfig)-> None:
        """
        Initializes the ChatbotRepository instance.

        Args:
            app_config (AppConfig): The application configuration.
            aws_config (AWSConfig): The AWS configuration.
        """
        self.aws_config = aws_config
        self.app_config = app_config

        self.table = self.__configure_dynamodb()


    def get_user_chat_sessions(self, user_id: str) -> List[ChatSession]:  
        """
        Retrieves all chat sessions for a specified user.

        Args:
            user_id (str): The ID of the user whose chats are being retrieved.

        Returns:
        List[ChatSession]: A list containing containing ChatSession objects.

        Raises:
            ServiceException: If there's an error while retrieving chats.
        """
        log.info('Retriving chats for user. user_id: %s', user_id)
        try:
            response = self.table.query(
                IndexName=self.app_config.chatbot_messages_gsi_name,
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            items = response.get('Items', [])
            chat_sessions = []
            for item in items:
                item = DataTypeUtils.convert_decimals_to_float_or_int(item)
                chat_sessions.append(from_dict(ChatSession, item))
            return chat_sessions

        except ClientError as e:
            log.exception('Failed to retrieve chats. user_id: %s', user_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve chats')


    def get_chat_messages(self, chat_id: str, limit: int, exclusive_start_key: dict = None) -> ChatMessageResponse:
        """
        Retrieves messages from a specific chat, with pagination support.

        Args:
            chat_id (str): The ID of the chat.
            limit (int): The maximum number of messages to retrieve.
            exclusive_start_key (Optional[Dict], optional): The key to start retrieving messages from for pagination. Defaults to None.

        Returns:
            ChatMessageResponse: A model object containing the list of ChatMessage objects and the last evaluated key for pagination.

        Raises:
            ServiceException: If there's an error while retrieving messages.
        """
        log.info('Retrieving messages for chat. chat_id: %s, limit: %s, exclusive_start_key: %s', chat_id, limit, exclusive_start_key)
        try:
            params = {
                'KeyConditionExpression': Key('chat_id').eq(chat_id),
                'Limit': limit,
                'ScanIndexForward': False
            }
            
            if exclusive_start_key:
                params['ExclusiveStartKey'] = exclusive_start_key

            response = self.table.query(**params)
            last_evaluated_key = DataTypeUtils.convert_decimals_to_float_or_int(response.get('LastEvaluatedKey'))

            messages = []
            for item in response.get('Items', []):
                item = DataTypeUtils.convert_decimals_to_float_or_int(item)
                messages.append(from_dict(ChatMessage, item))

            return ChatMessageResponse(
                messages=messages,
                last_evaluated_key=last_evaluated_key
            )
            
        except ClientError as e:
            log.exception('Failed to retrieve messages. chat_id: %s', chat_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
        raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve messages')
    

    def create_new_chat(self, item: Chat) -> None:
        """
        Creates a new chat in the DynamoDB table.

        Args:
            item (Chat): The chat item to be created.

        Raises:
            ServiceException: If there's an error while creating the chat.
        """
        log.info('Creating new chat. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
        try:
            self.table.put_item(Item=asdict(item))
            log.info('Successfully created chat. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
        except ClientError as e:
            log.exception('Failed to create chat. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to create chat.')
        

    def save_chat_interaction(self, chat_interaction: ChatInteraction) -> None:
        """
        Saves a chat interraction in the DynamoDB table.

        Args:
            chat_interaction (ChatInteraction): The chat interaction to be saved.

        Raises:
            ServiceException: If there's an error while saving the chat interaction.
        """
        log.info('Saving chat interaction. chat_id: %s, timestamp: %s', chat_interaction.chat_id, chat_interaction.timestamp)
        try: 
            self.table.put_item(Item=asdict(chat_interaction))
            log.info('Successfully saved chat interaction. chat_id: %s, timestamp: %s', chat_interaction.chat_id, chat_interaction.timestamp)
        except ClientError as e:
            log.exception('Failed to save chat interaction. chat_id: %s, timestamp: %s', chat_interaction.chat_id, chat_interaction.timestamp)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to save chat interaction.')
        

    def update_title_in_chat_context(self, chat_id: str, timestamp: int, title: str) -> Optional[ChatContext]:
        """
        Updates the title of the chat context.

        Args:
            chat_id (str): The ID of the chat context to update.
            timestamp (int): The timestamp of the chat context.
            title (str): The new title for the chat context.

        Returns:
            ChatContext: The updated ChatContext object if requested; otherwise, None.

        Raises:
            ServiceException: If there's an error while updating the title.
        """
        log.info('Updating chat context with title. chat_id: %s, timestamp: %s', chat_id, timestamp)
        try:
            response = self.table.update_item(
                Key={"chat_id": chat_id, "timestamp": timestamp},
                UpdateExpression="SET title = :title",
                ExpressionAttributeValues={":title": title},
                ReturnValues="ALL_NEW" 
            )
            log.info("Successfully updated chat with title. chat_id: %s, timestamp: %s", chat_id, timestamp)
            updated_item = response.get("Attributes", {})

            return from_dict(ChatContext, updated_item) 

        except ClientError as e:
            log.exception('Failed to update title. chat_id: %s, timestamp: %s', chat_id, timestamp)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to update title')


    def get_chat_context(self, chat_id: str, timestamp: int) -> ChatContext:
        """
        Retrieves chat context for a specific chat.

        Args:
            chat_id (str): The ID of the chat context.
            timestamp (int): The timestamp of the chat context.

        Returns:
            ChatContext: The context of the chat e.g, model_id, title.

        Raises:
            ServiceException: If there's an error while retrieving the chat context.
        """
        log.info('Retriving chat context for chat. chat_id: %s, timestamp: %s', chat_id, timestamp)
        try:
            response = self.table.get_item(
                Key={
                    'chat_id': chat_id,
                    'timestamp': timestamp
                }
            )
            item = response.get('Item', {})
            if not item:
                log.error('Chat context does not exist. chat_id: %s, timestamp: %s', chat_id, timestamp)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Chat context does not exists')
            
            log.info('Successfully retrieved chat info. chat_id: %s', chat_id)
            return from_dict(ChatContext, item)

        except ClientError as e:
            log.exception('Failed to retrieve chat context. chat_id: %s, timestamp: %s ', chat_id, timestamp)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to retrieve chat context')
        

    def get_chat_timestamp(self, user_id: str, chat_id: str) -> Optional[ChatCreationDate]:
        """
        Retrieves chat timestamp for a specific chat (used to update chat title).

        Args:
            chat_id (str): The ID of the chat session.
            user_id (str): The ID of the user.

        Returns:
            Optional[ChatCreationDate]: The ChatCreationDate object if found, otherwise None.

        Raises:
            ServiceException: If there's an error while retrieving the timestamp.
        """
        log.info('Getting chat creation timestamp. user_id: %s, chat_id: %s', user_id, chat_id)
        try:
            response = self.table.query(
                IndexName=self.app_config.chatbot_messages_gsi_name,
                KeyConditionExpression=Key('user_id').eq(user_id) & Key('chat_id').eq(chat_id)
            )
            items = response.get('Items', [])
            if items:  
                item = DataTypeUtils.convert_decimals_to_float_or_int(items[0])  
                return from_dict(ChatCreationDate, item)
            log.warning("No chat timestamp found. user_id: %s, chat_id: %s", user_id, chat_id)
            return None  
        
        except ClientError as e:
            log.exception('Failed to retrieve chat creation timestamp. user_id: %s, chat_id: %s', user_id, chat_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Could not retrieve chat creation timestamp')


    def __configure_dynamodb(self):
        """
        Configures and returns a DynamoDB table based on the current environment.

        Returns:
            The DynamoDB table object.`
        """
        resource = None

        if self.aws_config.is_local:
            resource = boto3.resource('dynamodb', region_name=self.aws_config.dynamodb_aws_region, endpoint_url = 'http://localhost:8000')
        else:
            config = Config(region_name = self.aws_config.dynamodb_aws_region)
            resource = boto3.resource('dynamodb', config = config)

        return resource.Table(self.app_config.chatbot_messages_table_name)
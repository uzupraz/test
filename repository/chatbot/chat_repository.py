import boto3 
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dataclasses import asdict
from dacite import from_dict
from typing import List

from model import Chat, ChatMessage, ChatSession, ParentInfo, ChatMessageResponse, ChildChat
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


    def get_user_chats(self, user_id: str) -> List[ChatSession]:  
        """
        Retrieves all chats for a specified user.

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
            chats = []
            for item in items:
                item = DataTypeUtils.convert_decimals_to_float_or_int(item)
                chats.append(from_dict(ChatSession, item))
            return chats

        except ClientError as e:
            log.exception('Failed to retrieve chats. user_id: %s', user_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Could not retrieve chats')


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
        log.info('Retrieving messages for chat. chat_id: %s', chat_id)
        try:
            params = {
                'KeyConditionExpression': Key('chat_id').eq(chat_id),
                'Limit': limit
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
        raise ServiceException(code, ServiceStatus.FAILURE, 'Could not retrieve messages')
    

    def create_new_chat(self, item: Chat) -> None:
        """
        Creates a new chat session in the DynamoDB table.

        Args:
            item (Chat): The chat item to be created.

        Raises:
            ServiceException: If there's an error while creating the chat session.
        """
        log.info('Creating new chat. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
        try:
            self.table.put_item(Item=asdict(item))
            log.info('Successfully created chat session. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
        except ClientError as e:
            log.exception('Failed to create chat session. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to create chat session')
        

    def save_message(self, item: ChildChat) -> None:
        """
        Saves a chat message in the DynamoDB table.

        Args:
            item (ChildChat): The chat message to be saved.

        Raises:
            ServiceException: If there's an error while saving the chat message.
        """
        log.info('Saving chat message. chat_id: %s, timestamp: %s', item.chat_id, item.timestamp)
        try: 
            self.table.put_item(Item=asdict(item))
            log.info('Successfully saved chat message. chat_id: %s, timestamp: %s', item.chat_id, item.timestamp)
        except ClientError as e:
            log.exception('Failed to save chat message. chat_id: %s, timestamp: %s', item.chat_id, item.timestamp)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to save chat message')
        

    def update_parent_chat_title(self, chat_id: str, title: str) -> None:
        """
        Updates the title of a parent chat session.

        Args:
            chat_id (str): The ID of the chat session to update.
            title (str): The new title for the chat session.

        Raises:
            ServiceException: If there's an error while updating the title.
        """
        try:
            self.table.update_item(
                Key={"chat_id": chat_id, "timestamp": 0},
                UpdateExpression="SET #title = if_not_exists(#title, :title)",
                ExpressionAttributeNames={"#title": "title"},
                ExpressionAttributeValues={":title": title}
            )
            log.info("Parent chat updated with title for chat_id: %s", chat_id)
        except ClientError as e:
            log.exception('Failed to update title. chat_id: %s', chat_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to update title')


    def get_parent_info(self, chat_id: str, timestamp: int) -> ParentInfo:
        """
        Retrieves parent chat information for a specific chat session.

        Args:
            chat_id (str): The ID of the chat session.
            timestamp (int): The timestamp of the parent chat.

        Returns:
            ParentInfo: The parent chat information.

        Raises:
            ServiceException: If there's an error while retrieving the parent info.
        """
        log.info('Retriving parent info for chat. chat_id: %s', chat_id)
        try:
            response = self.table.get_item(
                Key={
                    'chat_id': chat_id,
                    'timestamp': timestamp
                }
            )
            item = response.get('Item', {})
            if not item:
                log.error('Parent info does not exist. chat_id: %s', chat_id)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Chat does not exists')
            
            log.info('Successfully retrieved chat info. chat_id: %s', chat_id)
            return from_dict(ParentInfo, item)

        except ClientError as e:
            log.exception('Failed to retrieve parent info for chat. chat_id: %s', chat_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Could not retrieve parent info')


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
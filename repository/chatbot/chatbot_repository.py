import boto3 
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dataclasses import asdict
from typing import List, Dict

from model import Chat
from configuration import AWSConfig, AppConfig
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton, DataTypeUtils

log = common_ctrl.log

class ChatbotRepository(metaclass=Singleton):


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


    def get_user_chats(self, user_id: str) -> List[Dict[str, any]]:  
        """
        Retrieves all chats for a specified user.

        Args:
            user_id (str): The ID of the user whose chats are being retrieved.

        Returns:
        List[Dict[str, any]]: A list of dictionaries, each representing a chat record with fields
                              such as chat ID, timestamp.

        Raises:
            ServiceException: If there's an error while retrieving chats.
        """
        log.info('Retriving chats for user %s', user_id)
        try:
            response = self.table.query(
                IndexName=self.app_config.chatbot_gsi_name,
               KeyConditionExpression=Key('user_id').eq(user_id)
            )
            items = response.get('Items', [])

            converted_items = [DataTypeUtils.convert_decimals_to_float_or_int(item) for item in items]
            return converted_items

        except ClientError as e:
            log.exception('Failed to retrieve chats. user_id: %s', user_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Could not retrieve chats')
        

    def get_chat_title(self, chat_id: str, timestamp: int) -> str:
        """
        Retrieves the title of a specific chat based on chat ID and timestamp.

        Args:
            chat_id (str): The ID of the chat.
            timestamp (int): The timestamp of the chat.

        Returns:
            str: The title of the chat if found, otherwise an empty string.

        Raises:
            ServiceException: If there's an error while retrieving the chat title.
        """
        log.info('Retriving chat title for chat_id %s', chat_id)
        try:
            response = self.table.get_item(
                Key={
                    'chat_id': chat_id,
                    'timestamp': timestamp
                }
            )
            return response.get('Item', {}).get('title', '')
        except ClientError as e:
            log.exception('Failed to retrieve title. chat_id: %s', chat_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Could not retrieve title')
        

    def get_chat_messages(self, chat_id: str, limit: int, exclusive_start_key: dict = None) -> tuple[List[dict], dict]:
        """
        Retrieves messages from a specific chat, with pagination support.

        Args:
            chat_id (str): The ID of the chat.
            limit (int): The maximum number of messages to retrieve.
            exclusive_start_key (Optional[Dict], optional): The key to start retrieving messages from for pagination. Defaults to None.

        Returns:
            Tuple[List[Dict], Optional[Dict]]: A tuple containing a list of message items and the LastEvaluatedKey for pagination, if any.

        Raises:
            ServiceException: If there's an error while retrieving messages.
        """
        log.info('Retrieving chat messages for chat_id %s', chat_id)
        try:
            params = {
                'KeyConditionExpression': Key('chat_id').eq(chat_id),
                'Limit': limit
            }
            
            if exclusive_start_key:
                params['ExclusiveStartKey'] = exclusive_start_key

            response = self.table.query(**params)
            log.info('Successfully retrieved messages. chat_id: %s', chat_id)

            return response.get('Items', []), response.get('LastEvaluatedKey', None)
            
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
            log.info('Successfully created chat session. user_id: %s, chat_id:  %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
        except ClientError as e:
            log.exception('Failed to create chat session. user_id: %s, chat_id: %s, owner_id: %s', item.user_id, item.chat_id, item.owner_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to create chat session')


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
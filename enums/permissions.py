from enum import Enum


class ServicePermissions(Enum):
    DATA_TABLE_CREATE_ITEM='DATA_TABLE_CREATE_ITEM'
    DATA_TABLE_DELETE_ITEM='DATA_TABLE_DELETE_ITEM'
    CUSTOM_SCRIPT_SAVE_ITEM='CUSTOM_SCRIPT_SAVE_ITEM'
    CUSTOM_SCRIPT_RELEASE_ITEM='CUSTOM_SCRIPT_RELEASE_ITEM'
    CHATBOT_ACCESS='CHATBOT_ACCESS'


class ModelPermissions(Enum):
    """
    Enum defining model permissions with their model IDs
    """
    CLAUDE_HAIKU_ACCESS = 'anthropic.claude-3-haiku-20240307-v1:0'
    CLAUDE_SONNET_3_ACCESS = 'anthropic.claude-3-sonnet-20240229-v1:0'  
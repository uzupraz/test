from enum import Enum

class ModelPermissions(Enum):
    """
    Enum defining model permissions with their model IDs
    """
    CLAUDE_HAIKU_ACCESS = 'anthropic.claude-3-haiku-20240307-v1:0'
    CLAUDE_SONNET_3_ACCESS = 'anthropic.claude-3-sonnet-20240229-v1:0'



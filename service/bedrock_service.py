import boto3

from typing import List
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from model import Message
from configuration import AwsBedrockConfig

log = common_ctrl.log

tool_list = [
    {
        "toolSpec": {
            "name": "generate_title",
            "description": "Generate a concise title for the given content.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "A concise title (maximum 6 words) that captures the main topic."
                        },
                        "keywords": {
                            "type": "array",
                            "description": "Key terms or phrases that represent the main topics",
                            "items": {"type": "string"},
                            "maxItems": 3
                        },
                        "content_type": {
                            "type": "string",
                            "description": "The type of content being titled",
                            "enum": ["Question", "Discussion", "Statement", "Request"]
                        },
                    },
                    "required": ["title", "keywords", "content_type"]
                }
            }
        }
    },
]

class BedrockService:

    
    def __init__(self, bedrock_config: AwsBedrockConfig) -> None:
        """
        Initializes the BedrockService with the provided configuration.
        """
        self.bedrock_client = boto3.client(
            'bedrock-runtime'
        )
        self.bedrock_config = bedrock_config
        

    def send_prompt_to_model(self, model_id: str, prompt: str, messages: List[Message]):
        """
        Sends a prompt to the specified model and streams the response back using converse_stream.

        Args:
            model_id (str): The ID of the model to send the prompt to.
            prompt (str): The prompt that will be sent to the model.
            messages (List[Message]): A list of Message objects containing previous conversation history.

        Yields:
            str: A chunk of content from the model's response.

        Raises:
            ServiceException: If there is any failure in invoking the model or streaming the response.
        """
        log.info('Sending prompt to model. model_id: %s', model_id)
        try:
            # Convert Message objects to dictionaries with content as a list
            formatted_messages = [
                {"role": msg.role, "content": [{"text": msg.content}]}
                for msg in messages
            ]
            
            # Add the current prompt as a new message with content as a list
            formatted_messages.append({
                "role": "user", 
                "content": [{"text": prompt}]
            })
            
            # Use converse_stream method
            response = self.bedrock_client.converse_stream(
                modelId=model_id,
                messages=formatted_messages,
                inferenceConfig={
                    "maxTokens": 1000,
                    "temperature": 0.7  # Optional: adjust temperature as needed
                }
            )

            # Iterate through the response stream
            for event in response['stream']:
                if 'contentBlockDelta' in event:
                    content = event['contentBlockDelta'].get('delta', {}).get('text', '')
                    if content:
                        yield content

        except Exception as e:
            log.exception('Failed to stream response. model_id: %s', model_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Failed to stream response.')


    def generate_title(self, prompt: str) -> str:
        """
        Generates a structured title analysis for the provided prompt using tool configuration.

        Args:
            prompt (str): The prompt for which the title should be generated.

        Returns:
            str: The generated title for the prompt, or "Untitled" if no title could be generated.

        Raises:
            ServiceException: If there is any failure in generating the title using the Bedrock model.
        """
        log.info('Generating title for message')
        message = {
            "role": "user",
            "content": [
                {"text": f"<content>{prompt}</content>"},
                {
                    "text": "Please use the generate_title tool to generate the title JSON based on the content within the <content> tags."
                },
            ],
        }
        try:
            response = self.bedrock_client.converse(
                modelId=self.bedrock_config.default_model_id,
                messages=[message],
                inferenceConfig={
                    "maxTokens": 2000,
                    "temperature": 0
                },
                toolConfig={
                    "tools": tool_list,
                    "toolChoice": {
                        "tool": {
                            "name": "generate_title"
                        }
                    }
                }
            )

            # Extract the title from the response structure
            output = response.get("output", {})
            message_content = output.get("message", {}).get("content", [])
            
            # Locate the "toolUse" section in the content array
            for content_item in message_content:
                if "toolUse" in content_item:
                    title = content_item["toolUse"]["input"].get("title", "").strip()
                    return title if title else "Untitled"

            log.error("No title found in the response.")
            return "Untitled"
            
        except Exception as e:
            log.exception('Failed to generate title.')
            raise ServiceException(400, ServiceStatus.FAILURE, 'Failed to generate title.')

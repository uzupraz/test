import boto3
import json

from typing import List
from controller import common_controller as common_ctrl
from configuration import BedrockConfig
from exception import ServiceException
from enums import ServiceStatus
from model import Message

log = common_ctrl.log


class BedrockService:
    def __init__(self, bedrock_config: BedrockConfig) -> None:
        """
        Initializes the BedrockService with the provided configuration.

        Args:
            bedrock_config (BedrockConfig): The configuration object for Bedrock, containing region and other relevant settings.
        """
        self.bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=bedrock_config.region,
        )
        self.bedrock_config = bedrock_config

    def send_prompt_to_model(self, model_id: str, prompt: str, messages: List[Message]):
        """
        Sends a prompt to the specified model and streams the response back.

        This method formats the provided messages and prompt into a request body, sends it to the Bedrock model,
        and streams the response in chunks, yielding content as it becomes available.

        Args:
            model_id (str): The ID of the model to send the prompt to.
            prompt (str): The prompt that will be sent to the model.
            messages (List[Message]): A list of Message objects containing previous conversation history.

        Yields:
            str: A chunk of content from the model's response.

        Raises:
            ServiceException: If there is any failure in invoking the model or streaming the response.
        """
        try:
            # Convert Message objects to dictionaries
            formatted_messages = [
                {"role": msg.role, "content": msg.content}
                for msg in messages
            ]
            
            # Add the current prompt
            formatted_messages.append({"role": "user", "content": prompt})
            
            # Create the request body with the formatted messages
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": formatted_messages
            }

            response = self.bedrock_client.invoke_model_with_response_stream(
                modelId=model_id,
                body=json.dumps(request_body),
                contentType="application/json",
                accept="application/json"
            )

            for event in response['body']:
                chunk = json.loads(event['chunk']['bytes'].decode())
                if chunk.get('type') == 'content_block_delta':
                    content = chunk.get('delta', {}).get('text', '')
                    if content:
                        yield content

        except Exception:
            log.exception('Failed to stream response. model_id:', model_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to stream response.')


    def generate_title(self, message: str) -> str:
        """
        Generates a concise title for the provided message.

        This method sends a prompt to the Bedrock service asking it to generate a short, concise title
        based on the content of the provided message.

        Args:
            message (str): The message content for which the title should be generated.

        Returns:
            str: The generated title for the message, or "Untitled" if no title could be generated.

        Raises:
            ServiceException: If there is any failure in generating the title using the Bedrock model.
        """
        try:
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": f"Generate a short, concise title for the following message that captures its essence: "
                        f"'{message}'. Only include the essential keywords or phrase, without quotations and adding prefixes like Title"
                    }
                ]
            }

            response = self.bedrock_client.invoke_model(
                modelId='anthropic.claude-3-haiku-20240307-v1:0',
                body=json.dumps(request_body),
                contentType="application/json",
                accept="application/json"
            )

            # Read and decode the response body content
            response_body = json.loads(response['body'].read().decode('utf-8'))

            # Extract the title from the content array
            title = response_body.get("content", [{}])[0].get("text", "Untitled")

            # Return the generated title or a default if the title is missing
            return title if title else "Untitled"
        
        except Exception:
            log.exception('Failed to generate title.')
            raise ServiceException(500, ServiceStatus.FAILURE, 'Failed to generate title.')



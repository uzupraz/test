import json
import boto3
from botocore.exceptions import ClientError
from dacite import from_dict
from dataclasses import asdict

from typing import List
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from model import Message, GenerateModelRequest
from configuration import AwsBedrockConfig

log = common_ctrl.log


class BedrockService:


    content_type = 'application/json'


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
        log.info('Sending prompt to model. model_id: %s', model_id)
        try:
            chats= []
            for message in messages:
                chats.append(from_dict(Message, {"role": message.role, "content": message.content}))
                
            chats.append(from_dict(Message, {"role": "user", "content": prompt}))
            
            request_body = GenerateModelRequest(
                anthropic_version=self.bedrock_config.version,
                max_tokens=self.bedrock_config.max_tokens,
                messages=chats
            )

            response = self.bedrock_client.invoke_model_with_response_stream(
                modelId=model_id,
                body=json.dumps(asdict(request_body)).encode('utf-8'),
                contentType=self.content_type,
                accept=self.content_type,
            )

            for event in response['body']:
                chunk = json.loads(event['chunk']['bytes'].decode())
                if chunk.get('type') == 'content_block_delta':
                    content = chunk.get('delta', {}).get('text', '')
                    if content:
                        yield content

        except ClientError as e:
            log.exception('Failed to stream response. model_id:', model_id)
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to stream response.')


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
            request_body = GenerateModelRequest(
                anthropic_version=self.bedrock_config.version,
                max_tokens=self.bedrock_config.max_tokens,
                messages=[
                    Message(
                        role="user",
                        content=(
                            "Generate a short, concise title for the following message that captures its essence: "
                            f"'{message}'. Only include the essential keywords or phrase, without quotations and "
                            "adding prefixes like Title"
                        )
                    )
                ],
            )

            # Send the request to the Bedrock service
            response = self.bedrock_client.invoke_model(
                modelId=self.bedrock_config.model_id,
                body=json.dumps(asdict(request_body)).encode('utf-8'),  
                contentType=self.content_type,
                accept=self.content_type,
            )

            # Read and decode the response body content
            response_body = json.loads(response['body'].read().decode('utf-8'))

            # Extract the title from the content array
            title = response_body.get("content", [{}])[0].get("text", "Untitled")

            # Return the generated title or a default if the title is missing
            return title if title else "Untitled"

        except ClientError as e:
            log.exception('Failed to generate title.')
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to generate title.')

import json
import boto3
from botocore.exceptions import ClientError
from dacite import from_dict
from dataclasses import asdict

from typing import List
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from model import InteractionRecord, ModelInteractionRequest
from configuration import AwsBedrockConfig

log = common_ctrl.log


class BedrockService:


    content_type = 'application/json'
    PROMPT = "Generate a short, concise title for the following message that captures its essence: '{}'. Only include the essential keywords or phrase, without quotations and adding prefixes like Title"


    def __init__(self, bedrock_config: AwsBedrockConfig) -> None:
        """
        Initializes the BedrockService with the provided configuration.
        """
        self.bedrock_client = boto3.client(
            'bedrock-runtime'
        )
        self.bedrock_config = bedrock_config
        

    def send_prompt_to_model(self, model_id: str, prompt: str, interaction_records: List[InteractionRecord], system_prompt: str):
        """
        Sends a prompt to the specified model and streams the response back.

        This method formats the provided interaction_record and current prompt into a request body, sends it to the Bedrock model,
        and streams the response in chunks, yielding content as it becomes available.

        Args:
            model_id (str): The ID of the model to send the prompt to.
            prompt (str): The prompt that will be sent to the model.
            interaction_records (List[InteractionReord]): A list of InteractionReord objects containing previous conversation history.
            system_prompt (str, optional): A system-level instruction to set context for the conversation.

        Yields:
            str: A chunk of content from the model's response.

        Raises:
            ServiceException: If there is any failure in invoking the model or streaming the response.
        """
        log.info('Sending prompt to model. model_id: %s', model_id)
        try:
            chats = []

            # Add interaction records (previous conversation history)
            for message in interaction_records:
                chats.append(from_dict(InteractionRecord, {"role": message.role, "content": message.content}))

            # Add user's current prompt
            chats.append(from_dict(InteractionRecord, {"role": "user", "content": prompt}))

            request_body = ModelInteractionRequest(
                anthropic_version=self.bedrock_config.anthropic_version,
                max_tokens=self.bedrock_config.max_tokens,
                messages=chats,
                system=system_prompt
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
            log.exception('Failed to stream response. model_id: %s', model_id)
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
            request_body = ModelInteractionRequest(
                anthropic_version=self.bedrock_config.anthropic_version,
                max_tokens=self.bedrock_config.max_tokens,
                messages=[InteractionRecord(
                    role="user",
                    content=self.PROMPT.format(message)  
                )],
            )

            response = self.bedrock_client.invoke_model(
                modelId=self.bedrock_config.model_id,
                body=json.dumps(asdict(request_body)).encode('utf-8'),  
                contentType=self.content_type,
                accept=self.content_type,
            )

            response_body = json.loads(response['body'].read().decode('utf-8'))

            title = response_body.get("content", [{}])[0].get("text", "Untitled")

            return title if title else "Untitled"

        except ClientError as e:
            log.exception('Failed to generate title.')
            code = e.response['ResponseMetadata']['HTTPStatusCode']
            raise ServiceException(code, ServiceStatus.FAILURE, 'Failed to generate title.')
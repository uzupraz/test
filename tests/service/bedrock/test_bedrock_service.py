import json
import unittest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError
from io import BytesIO

from service.bedrock.bedrock_service import BedrockService
from exception import ServiceException
from model import InteractionRecord


class TestBedrockService(unittest.TestCase):


    def setUp(self) -> None:
        # Mock the Bedrock client and configuration
        self.mock_bedrock_client = MagicMock()
        self.mock_bedrock_config = MagicMock()
        
        # Set up mock return values for the configuration
        self.mock_bedrock_config.model_id = "test_model_id"
        self.mock_bedrock_config.anthropic_version = "anthropic_version"
        self.mock_bedrock_config.max_tokens = 1000


    def test_send_prompt_to_model_success(self):
        """
        Test case for send_prompt_to_model to verify successful response streaming.
        """
        # Mock dependencies
        bedrock_service = BedrockService(self.mock_bedrock_config)
        bedrock_service.bedrock_client = self.mock_bedrock_client

        # Mock the response from the model
        mock_response = {
            'body': [
                {'chunk': {'bytes': json.dumps({'type': 'content_block_delta', 'delta': {'text': 'Hello'}}).encode('utf-8')}},
                {'chunk': {'bytes': json.dumps({'type': 'content_block_delta', 'delta': {'text': 'World'}}).encode('utf-8')}},
            ]
        }
        self.mock_bedrock_client.invoke_model_with_response_stream.return_value = mock_response

        # Sample interaction records
        interaction_records = [
            InteractionRecord(role="user", content="Hi!"),
            InteractionRecord(role="assistant", content="Hello!"),
        ]

        # Execute
        result = list(bedrock_service.send_prompt_to_model("test_model_id", "How are you?", interaction_records))

        # Assert
        self.assertEqual(result, ["Hello", "World"])
        self.mock_bedrock_client.invoke_model_with_response_stream.assert_called_once()


    def test_send_prompt_to_model_failure(self):
        """
        Test case for send_prompt_to_model to handle streaming failure.
        """
        # Mock dependencies
        mock_bedrock_client = self.mock_bedrock_client
        bedrock_service = BedrockService(self.mock_bedrock_config)
        bedrock_service.bedrock_client = mock_bedrock_client

        mock_bedrock_client.invoke_model_with_response_stream.side_effect = ClientError(
            error_response={'ResponseMetadata': {'HTTPStatusCode': 500}}, operation_name='InvokeModel'
        )

        interaction_records = [InteractionRecord(role="user", content="Test")]

        # Execute & Assert
        with self.assertRaises(ServiceException) as context:
            list(bedrock_service.send_prompt_to_model("test_model_id", "Prompt", interaction_records))
        self.assertEqual(context.exception.status_code, 500)


    def test_generate_title_success(self):
        """
        Test case for generate_title to verify title generation.
        """
        # Mock dependencies
        mock_bedrock_client = self.mock_bedrock_client
        bedrock_service = BedrockService(self.mock_bedrock_config)
        bedrock_service.bedrock_client = mock_bedrock_client

        # Create a BytesIO stream to simulate a file-like object
        mock_response = {
            'body': BytesIO(json.dumps({'content': [{'text': 'Sample Title'}]}).encode('utf-8'))
        }
        mock_bedrock_client.invoke_model.return_value = mock_response

        # Execute
        result = bedrock_service.generate_title("Test message")

        # Assert
        self.assertEqual(result, "Sample Title")
        mock_bedrock_client.invoke_model.assert_called_once()


    def test_generate_title_failure(self):
        """
        Test case for generate_title to handle failures gracefully.
        """
        # Mock dependencies
        mock_bedrock_client = self.mock_bedrock_client
        bedrock_service = BedrockService(self.mock_bedrock_config)
        bedrock_service.bedrock_client = mock_bedrock_client

        mock_bedrock_client.invoke_model.side_effect = ClientError(
            error_response={'ResponseMetadata': {'HTTPStatusCode': 400}}, operation_name='InvokeModel'
        )

        # Execute & Assert
        with self.assertRaises(ServiceException) as context:
            bedrock_service.generate_title("Test message")
        self.assertEqual(context.exception.status_code, 400)


    def test_generate_title_returns_untitled_when_no_title_is_generated(self):
        """
        Test case for generate_title to verify it returns "Untitled" when no title is generated.
        """
        # Mock dependencies
        mock_bedrock_client = self.mock_bedrock_client
        bedrock_service = BedrockService(self.mock_bedrock_config)
        bedrock_service.bedrock_client = mock_bedrock_client

        # Mock the response from the model with missing content field
        mock_response = {
            'body': BytesIO(json.dumps({'content': [{}]}).encode('utf-8'))
        }
        mock_bedrock_client.invoke_model.return_value = mock_response

        # Execute
        result = bedrock_service.generate_title("Test message")

        # Assert that it returns "Untitled" when the content field is missing
        self.assertEqual(result, "Untitled")
        mock_bedrock_client.invoke_model.assert_called_once()
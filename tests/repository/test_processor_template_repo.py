import unittest
import dataclasses
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from configuration import AWSConfig, AppConfig
from repository import ProcessorTemplateRepo
from tests.test_utils import TestUtils
from model import ProcessorTemplate
from utils import Singleton
from exception import ServiceException


class TestProcessTemplateRepo(unittest.TestCase):


    test_resource_path = '/tests/resources/processor_templates/'


    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(ProcessorTemplateRepo)
        with patch('repository.processor_template_repo.ProcessorTemplateRepo._ProcessorTemplateRepo__configure_table') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.repo = ProcessorTemplateRepo(self.app_config, self.aws_config)
            self.repo.table = self.mock_table


    def test_get_all_templates_success(self):
        # Mock response from DynamoDB
        items = [TestUtils.get_file_content(self.test_resource_path + 'event_processor_template.json')]
        self.mock_table.scan.return_value = {'Items': items}

        # Call the method
        templates = self.repo.get_all_templates()

        # Assertions
        self.assertEqual(len(templates), 1)
        self.assertTrue(isinstance(templates[0], ProcessorTemplate))
        self.assertEqual(templates[0].template_id, 'file-event')
        self.assertEqual(templates[0].input, None)
        self.assertEqual(templates[0].output.media_type, 'text/plain')
        self.assertEqual(len(templates[0].parameters), 2)

        self.mock_table.scan.assert_called_once()


    def test_get_all_templates_empty(self):
        # Mock empty response from DynamoDB
        self.mock_table.scan.return_value = {'Items': []}

        # Call the method
        templates = self.repo.get_all_templates()

        # Assertions
        self.assertEqual(len(templates), 0)
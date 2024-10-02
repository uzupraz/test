import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from configuration import AWSConfig, AppConfig
from repository import UpdaterRepository
from model import Module
from utils import Singleton
from exception import ServiceException
from tests.test_utils import TestUtils


class TestUpdaterRepository(unittest.TestCase):

    test_resource_path = '/tests/resources/updater/'

    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(UpdaterRepository)

        # Patch the configuration methods
        with patch('repository.updater_repository.UpdaterRepository._UpdaterRepository__configure_table') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.repo = UpdaterRepository(self.app_config, self.aws_config)

    def test_get_csa_info_table_items_success_should_return_items(self):
        # Mock response from DynamoDB
        items = [TestUtils.get_file_content(self.test_resource_path + 'csa_machines_updater_items.json')]
        self.mock_table.query.return_value = {'Items': items}

        # Call the method
        owner_id = "owner123"
        machine_id = "machine123"
        info = self.repo.get_csa_info_table_items(owner_id, machine_id)

        # Assertions
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0]['machine_id'], 'machine123')
        self.assertEqual(info[0]['owner_id'], 'owner123')
        self.assertEqual(len(info[0]['module_version_list']), 1)

        self.mock_table.query.assert_called_once()

    def test_get_csa_info_table_items_empty_should_return_empty_list(self):
        # Mock empty response from DynamoDB
        self.mock_table.query.return_value = {'Items': []}

        # Call the method
        owner_id = "owner123"
        machine_id = "machine123"
        info = self.repo.get_csa_info_table_items(owner_id, machine_id)

        # Assertions
        self.assertEqual(len(info), 0)

    def test_get_csa_info_table_items_dynamodb_exception_should_raise_service_exception(self):
        # Mock a DynamoDB exception
        self.mock_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query')

        # Test exception handling
        owner_id = "owner123"
        machine_id = "machine123"
        with self.assertRaises(ServiceException) as context:
            self.repo.get_csa_info_table_items(owner_id, machine_id)

        # Assertions
        self.assertEqual('Failed to retrive table items', context.exception.message)

    def test_get_release_info_success_should_return_item(self):
        # Mock response from DynamoDB
        items = [TestUtils.get_file_content(self.test_resource_path + 'module_version_updater_items.json')]
        self.mock_table.query.return_value = {'Items': items}

        # Call the method
        module_name = "module_name"
        info = self.repo.get_release_info(module_name)

        # Assertions
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0]['module_name'], 'module_name')
        self.assertEqual(info[0]['checksum'], 'checksum123')
        self.assertEqual(info[0]['version'], '1.0.0')

        self.mock_table.query.assert_called_once()

    def test_get_release_info_empty_should_return_empty_list(self):
        # Mock empty response from DynamoDB
        self.mock_table.query.return_value = {'Items': []}

        # Call the method
        module_name = "module_name"
        info = self.repo.get_release_info(module_name)

        # Assertions
        self.assertEqual(len(info), 0)

    def test_get_release_info_dynamodb_exception_should_raise_service_exception(self):
        # Mock a DynamoDB exception
        self.mock_table.query.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query')

        # Test exception handling
        module_name = "module_name"
        with self.assertRaises(ServiceException) as context:
            self.repo.get_release_info(module_name)

        # Assertions
        self.assertEqual('Failed to retrive table items', context.exception.message)

    def test_update_module_version_list_success_should_update_item(self):
        # Call the method
        machine_id = "machine123"
        owner_id = "owner123"
        module_version_list = [Module(version="1.1.0", module_name="module_name")]

        self.repo.update_module_version_list(machine_id, owner_id, module_version_list)

        # Assertions
        self.mock_table.update_item.assert_called_once_with(
            Key={'machine_id': machine_id, 'owner_id': owner_id},
            UpdateExpression="SET module_version_list = :updateList",
            ExpressionAttributeValues={':updateList': module_version_list}
        )

    def test_update_module_version_list_dynamodb_exception_should_raise_service_exception(self):
        # Mock a DynamoDB exception
        self.mock_table.update_item.side_effect = ClientError(
            {'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'update_item')

        # Test exception handling
        machine_id = "machine123"
        owner_id = "owner123"
        module_version_list = [Module(version="1.1.0", module_name="module_name")]

        with self.assertRaises(ServiceException) as context:
            self.repo.update_module_version_list(machine_id, owner_id, module_version_list)

        # Assertions
        self.assertEqual('Failed to update module_version list description', context.exception.message)



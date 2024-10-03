import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from repository import CsaUpdaterRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from model import MachineInfo, ModuleInfo, Module
from utils import Singleton


class TestCsaUpdaterRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/csa_updater/'


    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(CsaUpdaterRepository)
        with patch('repository.csa_repository.csa_updater_repository.CsaUpdaterRepository._CsaUpdaterRepository__configure_table') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.repo = CsaUpdaterRepository(self.app_config, self.aws_config)


    def test_get_csa_machines_info_success(self):
        # Mock DynamoDB response
        items = [TestUtils.get_file_content(self.test_resource_path + 'csa_machines_updater_items.json')]
        self.mock_table.query.return_value = {"Items": items}

        # Call method
        machine_info = self.repo.get_csa_machines_info("owner123", "machine123")

        # Assertions
        self.assertEqual(len(machine_info), 1)
        self.assertTrue(isinstance(machine_info[0], MachineInfo))
        self.assertEqual(machine_info[0].owner_id, "owner123")
        self.assertEqual(machine_info[0].machine_id, "machine123")
        self.assertEqual(machine_info[0].platform, "platform")
        self.assertEqual(machine_info[0].modules[0].module_name, "module_name")
        self.assertEqual(machine_info[0].modules[0].version, "1.1.0")

        self.mock_table.query.assert_called_once()


    def test_get_csa_machines_info_dynamodb_exception(self):
        # Mock DynamoDB ClientError
        self.mock_table.query.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "query"
        )

        # Test exception handling
        with self.assertRaises(ServiceException) as context:
            self.repo.get_csa_machines_info("owner123", "machine123")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.message, "Could not retrieve owner's machine info")


    def test_get_csa_module_versions_success(self):
        # Mock DynamoDB response
        items = [TestUtils.get_file_content(self.test_resource_path + 'module_version_updater_items.json')]
        self.mock_table.query.return_value = {"Items": items}

        # Call method
        module_info = self.repo.get_csa_module_versions("module_name")

        # Assertions
        self.assertEqual(len(module_info), 1)
        self.assertTrue(isinstance(module_info[0], ModuleInfo))
        self.assertEqual(module_info[0].module_name, "module_name")
        self.assertEqual(module_info[0].version, "1.0.0")
        self.assertEqual(module_info[0].checksum, "checksum123")

        self.mock_table.query.assert_called_once()


    def test_get_csa_module_versions_dynamodb_exception(self):
        # Mock DynamoDB ClientError
        self.mock_table.query.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "query"
        )

        # Test exception handling
        with self.assertRaises(ServiceException) as context:
            self.repo.get_csa_module_versions("module_name")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.message, "Could not retrieve owner's machine info")


    def test_update_modules_success(self):
        # Mock successful update
        self.mock_table.update_item.return_value = {}

        # Call method
        modules = [Module(module_name="module_name", version="1.0.0")]
        self.repo.update_modules("owner123", "machine123", modules)

        # Assertions
        self.mock_table.update_item.assert_called_once_with(
            Key={'owner_id': 'owner123', 'machine_id': 'machine123'},
            UpdateExpression="SET modules = :updateList",
            ExpressionAttributeValues={':updateList': modules}
        )


    def test_update_modules_dynamodb_exception(self):
        # Mock DynamoDB ClientError
        self.mock_table.update_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "update_item"
        )

        # Test exception handling
        modules = [Module(module_name="module_name", version="1.0.0")]
        with self.assertRaises(ServiceException) as context:
            self.repo.update_modules("owner123", "machine123", modules)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.message, "Failed to update modules")


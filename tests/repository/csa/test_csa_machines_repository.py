import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from repository import CsaMachinesRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from model import MachineInfo, ModuleInfo, Module
from utils import Singleton


class TestCsaMachinesRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/csa/'


    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(CsaMachinesRepository)
        with patch('repository.csa.csa_machines_repository.CsaMachinesRepository._CsaMachinesRepository__configure_dynamodb') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.repo = CsaMachinesRepository(self.app_config, self.aws_config)


    def test_get_csa_machine_info_success(self):
        # Mock DynamoDB response
        item = TestUtils.get_file_content(self.test_resource_path + 'csa_machines_updater_items.json')
        self.mock_table.get_item.return_value = {"Item": item} 

        # Call method
        machine_info = self.repo.get_csa_machine_info("owner123", "machine123")

        # Assertions
        self.assertIsInstance(machine_info, MachineInfo)  
        self.assertEqual(machine_info.owner_id, "owner123")
        self.assertEqual(machine_info.machine_id, "machine123")
        self.assertEqual(machine_info.platform, "platform")
        self.assertEqual(machine_info.modules[0].module_name, "module_name")
        self.assertEqual(machine_info.modules[0].version, "1.1.0")

        # Verify the mock was called with the expected arguments
        self.mock_table.get_item.assert_called_once_with(Key={"owner_id": "owner123", "machine_id": "machine123"})



    def test_get_csa_machine_info_dynamodb_exception(self):
        # Mock DynamoDB ClientError
        self.mock_table.get_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "query"
        )

        # Test exception handling
        with self.assertRaises(ServiceException) as context:
            self.repo.get_csa_machine_info("owner123", "machine123")

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


import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from repository import CsaModuleVersionsRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from model import MachineInfo, ModuleInfo, Module
from utils import Singleton


class TestCsaModuleVersionsRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/csa/'


    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(CsaModuleVersionsRepository)
        with patch('repository.csa.csa_module_versions_repository.CsaModuleVersionsRepository._CsaModuleVersionsRepository__configure_dynamodb') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.repo = CsaModuleVersionsRepository(self.app_config, self.aws_config)


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
        self.assertEqual(context.exception.message, "Could not retrieve modules")



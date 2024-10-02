import unittest
from unittest.mock import MagicMock, patch
from packaging import version

from model import Module, TargetList, UpdateResponse
from repository import UpdaterRepository
from service import UpdaterService, S3AssetsService
from configuration import S3AssetsFileConfig, AppConfig, AWSConfig
from exception import ServiceException
from utils import Singleton
from enums import ServiceStatus

class TestUpdaterService(unittest.TestCase):

    TEST_RESOURCE_PATH = '/tests/resources/updater/'

    def setUp(self) -> None:
        Singleton.clear_instance(UpdaterRepository)
        Singleton.clear_instance(S3AssetsService)
        Singleton.clear_instance(UpdaterService)

        # Create mock configurations
        self.app_config = AppConfig()
        self.aws_config = AWSConfig()
        self.aws_config.is_local = True
        self.aws_config.dynamodb_aws_region = "local"
        
        self.s3_assets_file_config = S3AssetsFileConfig(
            bucket_name="test-bucket",
            region="us-east-1"
        )
        
        # Initialize repository with mock configs
        self.repository = UpdaterRepository(self.app_config, self.aws_config)
        
        # Initialize service
        self.updater_service = UpdaterService(self.repository, self.s3_assets_file_config)

    def tearDown(self) -> None:
        Singleton.clear_instance(UpdaterRepository)
        Singleton.clear_instance(S3AssetsService)
        Singleton.clear_instance(UpdaterService)

    @patch('service.updater_service.UpdaterRepository.get_csa_info_table_items')
    @patch('service.updater_service.UpdaterRepository.get_release_info')
    @patch('service.updater_service.S3AssetsService.generate_download_pre_signed_url')
    @patch('service.updater_service.UpdaterRepository.update_module_version_list')
    def test_get_target_list(self, mock_update_module_version_list, 
                                       mock_generate_url, mock_get_release_info, 
                                       mock_get_csa_info):
        """
        Tests the happy path for get_target_list method.
        """
        # Setup test data
        machine_id = "test_machine"
        owner_id = "test_owner"
        machine_info_list = [
            {"module_name": "module1", "version": "1.0.0"},
            {"module_name": "module2", "version": "1.9.0"}
        ]
        
        # Mock CSA table response
        mock_get_csa_info.return_value = [{
            'module_version_list': [
                {'module_name': 'module1', 'version': '1.1.0'},
                {'module_name': 'module2', 'version': '2.0.0'}
            ],
            'platform': 'windows'
        }]
        
        # Mock release info response
        mock_get_release_info.side_effect = [
            [{'version': '1.1.0', 'checksum': 'checksum1'}],
            [{'version': '2.0.0', 'checksum': 'checksum2'}]
        ]
        
        # Mock S3 URL generation
        mock_generate_url.return_value = "https://test-url.com"
        
        # Execute the method
        result = self.updater_service.get_target_list(machine_id, owner_id, machine_info_list)
        
        # Assertions
        self.assertIsInstance(result, UpdateResponse)
        self.assertEqual(len(result.target_list), 2)
        
        modules = {item.module_name: item for item in result.target_list}
        self.assertIn('module1', modules)
        self.assertIn('module2', modules)
        self.assertEqual(modules['module1'].version, "1.1.0")
        self.assertEqual(modules['module2'].version, "2.0.0")
        
        # Verify mock calls
        mock_get_csa_info.assert_called_once_with(owner_id=owner_id, machine_id=machine_id)
        self.assertEqual(mock_get_release_info.call_count, 2)
        mock_update_module_version_list.assert_called_once()

    @patch('service.updater_service.UpdaterRepository.get_csa_info_table_items')
    def test_get_target_list_no_csa_info(self, mock_get_csa_info):
        """
        Tests get_target_list when no CSA info is found.
        """
        mock_get_csa_info.return_value = []
        
        with self.assertRaises(ServiceException) as context:
            self.updater_service.get_target_list("test_machine", "test_owner", [])
        
        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)

    def test_get_machine_versions(self):
        """
        Tests _get_machine_versions helper method.
        """
        machine_info_list = [
            {"module_name": "module1", "version": "1.0.0"},
            {"module_name": "module2", "version": "2.0.0"}
        ]
        
        result = self.updater_service._get_machine_versions(machine_info_list)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result["module1"], version.parse("1.0.0"))
        self.assertEqual(result["module2"], version.parse("2.0.0"))

    def test_get_next_version(self):
        """
        Tests _get_next_version helper method for different scenarios.
        """
        available_versions = [
            version.parse("1.0.0"),
            version.parse("1.0.1"),
            version.parse("1.1.0"),
            version.parse("1.1.1")
        ]
        
        # Test catching up to current version
        current_version = version.parse("1.1.0")
        machine_version = version.parse("1.0.0")
        result = self.updater_service._get_next_version(current_version, available_versions, machine_version)
        self.assertEqual(result, version.parse("1.1.0"))
        
        # Test when machine is already up to date
        machine_version = version.parse("1.1.0")
        result = self.updater_service._get_next_version(current_version, available_versions, machine_version)
        self.assertEqual(result, version.parse("1.1.1"))
        
        machine_version = version.parse("1.1.1")
        result = self.updater_service._get_next_version(current_version, available_versions, machine_version)
        self.assertEqual(result, version.parse("1.1.1"))

    def test_generate_s3_key(self):
        """
        Tests _generate_s3_key helper method.
        """
        module_name = "test_module"
        next_version = version.parse("1.1.0")
        
        # Test Windows platform
        windows_key = self.updater_service._generate_s3_key(module_name, next_version, "windows")
        self.assertEqual(windows_key, "assets/system/csa_modules/test_module/windows/test_module.1.1.0.zip")
        
        # Test Linux platform
        linux_key = self.updater_service._generate_s3_key(module_name, next_version, "linux")
        self.assertEqual(linux_key, "assets/system/csa_modules/test_module/linux/test_module.1.1.0.tar")

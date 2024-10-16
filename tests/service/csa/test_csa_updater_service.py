import unittest
from unittest.mock import MagicMock, patch
from packaging import version

from model import Module, UpdateResponse, MachineInfo, ModuleInfo
from repository import CsaMachinesRepository, CsaModuleVersionsRepository
from service import CsaUpdaterService, S3AssetsService
from configuration import S3AssetsFileConfig, AppConfig, AWSConfig
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton
from tests.test_utils import TestUtils


class TestCsaUpdaterService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/csa/'


    def setUp(self) -> None:
        Singleton.clear_instance(CsaMachinesRepository)
        Singleton.clear_instance(CsaModuleVersionsRepository)
        Singleton.clear_instance(S3AssetsService)
        Singleton.clear_instance(CsaUpdaterService)

        self.app_config_mock = MagicMock(spec=AppConfig)
        self.aws_config_mock = MagicMock(spec=AWSConfig)
        self.s3_assets_file_config_mock = MagicMock(spec=S3AssetsFileConfig)
        self.csa_machines_repository_mock = MagicMock(spec=CsaMachinesRepository)
        self.csa_module_versions_repository_mock = MagicMock(spec=CsaModuleVersionsRepository)
        self.s3_service_mock = MagicMock(spec=S3AssetsService)
        self.csa_updater_service = CsaUpdaterService(
            self.csa_machines_repository_mock,
            self.csa_module_versions_repository_mock,
            self.s3_assets_file_config_mock
        )
        self.csa_updater_service.s3_asset_service = self.s3_service_mock

        # Load test data from JSON files
        self.machine_info_item = TestUtils.get_file_content(self.TEST_RESOURCE_PATH + 'csa_machines_updater_items.json')
        self.module_version_item = TestUtils.get_file_content(self.TEST_RESOURCE_PATH + 'module_version_updater_items.json')

        # Extract common test data from JSON
        self.owner_id = self.machine_info_item['owner_id']
        self.machine_id = self.machine_info_item['machine_id']
        self.platform = self.machine_info_item['platform']
        self.module_name = self.machine_info_item['modules'][0]['module_name']


    def test_get_targets_success(self):
        machine_info = MachineInfo(
            owner_id=self.machine_info_item['owner_id'],
            machine_id=self.machine_info_item['machine_id'],
            modules=[Module(**module) for module in self.machine_info_item['modules']],
            platform=self.machine_info_item['platform']
        )
        self.csa_machines_repository_mock.get_csa_machine_info.return_value = machine_info

        current_version = self.machine_info_item['modules'][0]['version']  
        higher_version = "1.2.0"  
        
        module_info = ModuleInfo(
            module_name=self.module_version_item['module_name'],
            version=higher_version,
            checksum=self.module_version_item['checksum']
        )
        self.csa_module_versions_repository_mock.get_csa_module_versions.return_value = [module_info]

        self.s3_service_mock.generate_download_pre_signed_url.return_value = "https://fakeurl.com"

        response = self.csa_updater_service.get_targets(
            self.owner_id, 
            self.machine_id, 
            self.machine_info_item['modules']
        )

        # Verify
        self.assertIsInstance(response, UpdateResponse)
        self.assertEqual(len(response.targets), 1)
        target_item = response.targets[0]
        self.assertEqual(target_item.module_name, self.module_name)
        self.assertEqual(target_item.version, higher_version)
        self.assertEqual(target_item.checksum, self.module_version_item['checksum'])
        self.assertEqual(target_item.presigned_url, "https://fakeurl.com")
        
        # Verify repository calls
        self.csa_machines_repository_mock.update_modules.assert_called_once()
        self.csa_machines_repository_mock.get_csa_machine_info.assert_called_once_with(self.owner_id, self.machine_id)
        self.csa_module_versions_repository_mock.get_csa_module_versions.assert_called_once_with(self.module_name)


    @patch.object(CsaUpdaterService, '_get_next_version')
    def test_get_targets_no_update_required(self, mock_get_next_version):
        module_name = "test_module"
        current_version = "1.1.0"
        
        machine_info = MachineInfo(
            owner_id=self.owner_id,
            machine_id=self.machine_id,
            modules=[Module(module_name=module_name, version=current_version)],
            platform=self.platform
        )
        self.csa_machines_repository_mock.get_csa_machine_info.return_value = machine_info

        module_info = ModuleInfo(
            module_name=module_name,
            version=current_version,
            checksum="test_checksum"
        )
        self.csa_module_versions_repository_mock.get_csa_module_versions.return_value = [module_info]

        mock_get_next_version.return_value = version.parse(current_version)

        response = self.csa_updater_service.get_targets(
            self.owner_id, 
            self.machine_id, 
            [{"module_name": module_name, "version": current_version}]
        )

        self.assertIsInstance(response, UpdateResponse)
        self.assertEqual(len(response.targets), 1)
        self.assertEqual(response.targets[0].version, current_version)


    def test_get_next_version(self):
        test_cases = [
            ("1.0.0", ["1.0.0", "1.0.1", "1.0.2", "1.1.0", "2.0.0"], "1.0.2"),  # Patch update
            ("1.0.2", ["1.0.2", "1.1.0", "1.2.0", "2.0.0"], "1.1.0"),  # Minor update
            ("1.1.0", ["1.1.0", "1.1.1", "2.0.0"], "1.1.1"),  # Patch update when minor is available
            ("1.1.1", ["1.1.1", "2.0.0"], "1.1.1"),  # No update available
        ]

        for current, available, expected in test_cases:
            with self.subTest(current=current, available=available, expected=expected):
                result = self.csa_updater_service._get_next_version(
                    version.parse(current),
                    [version.parse(v) for v in available]
                )
                self.assertEqual(str(result), expected)


    @patch.object(CsaUpdaterService, '_get_next_version')
    def test_get_targets_multiple_modules(self, mock_get_next_version):
        module1 = {"name": "module1", "current": "1.0.0", "next": "1.1.0"}
        module2 = {"name": "module2", "current": "2.0.0", "next": "2.0.1"}
        module3 = {"name": "module3", "current": "3.0.0", "next": "3.0.0"}  

        machine_info = MachineInfo(
            owner_id=self.owner_id,
            machine_id=self.machine_id,
            modules=[Module(module_name=m["name"], version=m["current"]) for m in [module1, module2, module3]],
            platform=self.platform
        )
        self.csa_machines_repository_mock.get_csa_machine_info.return_value = machine_info

        def mock_get_module_versions(module_name):
            for m in [module1, module2, module3]:
                if m["name"] == module_name:
                    return [ModuleInfo(module_name=m["name"], version=m["next"], checksum=f"{m['name']}_checksum")]
            return []

        self.csa_module_versions_repository_mock.get_csa_module_versions.side_effect = mock_get_module_versions

        def mock_get_next_version_impl(current, available):
            current_str = str(current)
            for m in [module1, module2, module3]:
                if m["current"] == current_str:
                    return version.parse(m["next"])
            return current  

        mock_get_next_version.side_effect = mock_get_next_version_impl

        self.s3_service_mock.generate_download_pre_signed_url.return_value = "https://fakeurl.com"

        response = self.csa_updater_service.get_targets(
            self.owner_id, 
            self.machine_id, 
            [{"module_name": m["name"], "version": m["current"]} for m in [module1, module2, module3]]
        )

        self.assertIsInstance(response, UpdateResponse)
        self.assertEqual(len(response.targets), 3)
        self.assertEqual(response.targets[0].version, module1["next"])
        self.assertEqual(response.targets[1].version, module2["next"])
        self.assertEqual(response.targets[2].version, module3["next"])  

        self.assertEqual(mock_get_next_version.call_count, 3)

        expected_update_list = [
            {"module_name": m["name"], "version": m["next"]}
            for m in [module1, module2, module3]
        ]
        self.csa_machines_repository_mock.update_modules.assert_called_once_with(
            self.owner_id, self.machine_id, expected_update_list
        )


    def test_get_next_version_patch_update(self):
        current_version = version.parse("1.0.0")
        available_versions = [
            version.parse(v) for v in ["1.0.0", "1.0.1", "1.0.2", "1.1.0"]
        ]
        
        next_version = self.csa_updater_service._get_next_version(
            current_version, available_versions
        )
        
        self.assertEqual(str(next_version), "1.0.2")


    def test_get_next_version_minor_update(self):
        current_version = version.parse("1.0.2")
        available_versions = [
            version.parse(v) for v in ["1.0.2", "1.1.0", "1.1.1", "2.0.0"]
        ]
        
        next_version = self.csa_updater_service._get_next_version(
            current_version, available_versions
        )
        
        self.assertEqual(str(next_version), "1.1.0")  


    def test_generate_asset_key_windows(self):
        module_name = "test_module"
        next_version = version.parse("1.0.1")
        platform = "windows"
        
        key = self.csa_updater_service._generate_asset_key(
            module_name, next_version, platform
        )
        
        expected_key = f"system/csa_modules/{module_name}/{platform}/{module_name}.1.0.1.zip"
        self.assertEqual(key, expected_key)


    def test_generate_asset_key_linux(self):
        module_name = "test_module"
        next_version = version.parse("1.0.1")
        platform = "linux"
        
        key = self.csa_updater_service._generate_asset_key(
            module_name, next_version, platform
        )
        
        expected_key = f"system/csa_modules/{module_name}/{platform}/{module_name}.1.0.1.tar"
        self.assertEqual(key, expected_key)


    def test_create_targets_item(self):
            self.s3_service_mock.generate_download_pre_signed_url.return_value = "https://fakeurl.com"
            
            next_version = version.parse("1.0.1")
            module_version_info = [ModuleInfo(module_name=self.module_name, version="1.0.1", checksum="abc123")]
            
            target_item = self.csa_updater_service._create_targets_item(
                self.module_name, next_version, module_version_info, self.platform
            )

            self.assertEqual(target_item.module_name, self.module_name)
            self.assertEqual(target_item.version, "1.0.1")
            self.assertEqual(target_item.presigned_url, "https://fakeurl.com")
            self.assertEqual(target_item.checksum, "abc123")


    


    
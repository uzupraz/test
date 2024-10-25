import unittest
from unittest.mock import MagicMock
from packaging import version

from dacite import from_dict
from model import Module, UpdateResponse, MachineInfo, ModuleInfo, Targets
from repository import CsaMachinesRepository, CsaModuleVersionsRepository
from service import CsaUpdaterService, S3AssetsService
from configuration import S3AssetsFileConfig
from exception import ServiceException
from utils import Singleton
from tests.test_utils import TestUtils


class TestCsaUpdaterService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/csa/'
    FAKE_PRESIGNED_URL = "https://fakeurl.com"
    FAKE_CHECKSUM = 'fake_checksum'


    def setUp(self) -> None:
        Singleton.clear_instance(CsaMachinesRepository)
        Singleton.clear_instance(CsaModuleVersionsRepository)
        Singleton.clear_instance(S3AssetsService)
        Singleton.clear_instance(CsaUpdaterService)

        self.s3_assets_file_config = MagicMock(spec=S3AssetsFileConfig)
        self.csa_machines_repo = MagicMock(spec=CsaMachinesRepository)
        self.csa_module_versions_repo = MagicMock(spec=CsaModuleVersionsRepository)
        self.s3_service_mock = MagicMock(spec=S3AssetsService)
        self.csa_updater_service = CsaUpdaterService(
            self.csa_machines_repo,
            self.csa_module_versions_repo,
            self.s3_assets_file_config
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


    def tearDown(self) -> None:
        self.csa_machines_repo = None
        self.csa_module_versions_repo = None


    def test_get_targets_success(self):
        """
        Test case for retrieving target updates for machine modules.

        Case: Updates are available for machine modules.
        Expected Result: The method returns an UpdateResponse with correct target information 
        (module name, version, checksum, and pre-signed URL) and updates the repository.
        """
        # Mock
        machine_info = from_dict(MachineInfo, self.machine_info_item)
        self.csa_machines_repo.get_csa_machine_info.return_value = machine_info

        higher_version = "1.2.0"  
        
        module_info = ModuleInfo(
            module_name=self.module_version_item['module_name'],
            version=higher_version,
            checksum=self.module_version_item['checksum']
        )
        self.csa_module_versions_repo.get_csa_module_versions.return_value = [module_info]

        self.s3_service_mock.generate_download_pre_signed_url.return_value = self.FAKE_PRESIGNED_URL

        # Call method
        response = self.csa_updater_service.get_targets(
            self.owner_id, 
            self.machine_id, 
            self.machine_info_item['modules']
        )

        # Assetrions
        self.assertIsInstance(response, UpdateResponse)
        self.assertEqual(len(response.targets), 1)
        target_item = response.targets[0]
        self.assertEqual(target_item.module_name, self.module_name)
        self.assertEqual(target_item.version, higher_version)
        self.assertEqual(target_item.checksum, self.module_version_item['checksum'])
        self.assertEqual(target_item.presigned_url, self.FAKE_PRESIGNED_URL)
        
        self.csa_machines_repo.update_modules.assert_called_once()
        self.csa_machines_repo.get_csa_machine_info.assert_called_once_with(self.owner_id, self.machine_id)
        self.csa_module_versions_repo.get_csa_module_versions.assert_called_once_with(self.module_name)
        self.s3_service_mock.generate_download_pre_signed_url.assert_called_once()


    def test_get_targets_no_update_required(self):
        """
        Test case for handling scenarios where no updates are required.

        Case: Current version matches the latest version available.
        Expected Result: The method returns an UpdateResponse with the same version, 
        indicating no updates are necessary.
        """
        # Mock
        module_name = "test_module"
        current_version = "1.1.0"
        
        machine_info = from_dict(MachineInfo, self.machine_info_item)
        self.csa_machines_repo.get_csa_machine_info.return_value = machine_info
        
        module_info = ModuleInfo(
            module_name=module_name,
            version=current_version,
            checksum="test_checksum"
        )
        self.csa_module_versions_repo.get_csa_module_versions.return_value = [module_info]

        # Call Method
        response = self.csa_updater_service.get_targets(
            self.owner_id, 
            self.machine_id, 
            [{"module_name": module_name, "version": current_version}]
        )

        # Assertions
        self.assertIsInstance(response, UpdateResponse)
        self.assertEqual(len(response.targets), 1)
        self.assertEqual(response.targets[0].module_name, self.module_name)
        self.assertEqual(response.targets[0].version, current_version)
        self.assertEqual(response.targets[0].checksum, module_info.checksum)

        self.csa_machines_repo.update_modules.assert_called_once()
        self.csa_machines_repo.get_csa_machine_info.assert_called_once_with(self.owner_id, self.machine_id)
        self.csa_module_versions_repo.get_csa_module_versions.assert_called_once_with(self.module_name)


    def test_get_targets_multiple_modules(self):
        """
        Test case for retrieving updates for multiple modules.

        Case: Multiple modules require updates.
        Expected Result: The method returns an UpdateResponse with the correct target information 
        for each module and updates the repository with the correct module versions.
        """
        # Mock
        modules = [
            {"name": "module1", "current": "1.0.0", "next": "1.1.0"},
            {"name": "module2", "current": "2.0.0", "next": "2.0.1"},
            {"name": "module3", "current": "3.0.0", "next": "3.0.0"} # No updates required for module3
        ]

        machine_info = MachineInfo(
            owner_id=self.machine_info_item['owner_id'],
            machine_id=self.machine_info_item['machine_id'],
            modules=[Module(module_name=m["name"], version=m["current"]) for m in modules],
            platform=self.machine_info_item['platform']
        )
        self.csa_machines_repo.get_csa_machine_info.return_value = machine_info

        def mock_get_module_versions(module_name):
            for m in modules:
                if m["name"] == module_name:
                    return [ModuleInfo(module_name=m["name"], version=m["next"], checksum=self.FAKE_CHECKSUM)]
            return []

        self.csa_module_versions_repo.get_csa_module_versions.side_effect = mock_get_module_versions

        self.s3_service_mock.generate_download_pre_signed_url.return_value = self.FAKE_PRESIGNED_URL

        # Call Method
        response = self.csa_updater_service.get_targets(
            self.owner_id, 
            self.machine_id, 
            [{"module_name": m["name"], "version": m["current"]} for m in modules]
        )

        # Assertions
        self.assertIsInstance(response, UpdateResponse)
        self.assertEqual(len(response.targets), len(modules))

        for i, module in enumerate(modules):
            self.assertEqual(response.targets[i].module_name, module["name"])
            self.assertEqual(response.targets[i].version, module["next"])
            self.assertEqual(response.targets[i].checksum, self.FAKE_CHECKSUM)
            self.assertEqual(response.targets[i].presigned_url, self.FAKE_PRESIGNED_URL)

        expected_update_list = [
            {"module_name": m["name"], "version": m["next"]}
            for m in modules
        ]
        self.csa_machines_repo.update_modules.assert_called_once_with(
            self.owner_id, self.machine_id, expected_update_list
        )
        self.csa_machines_repo.get_csa_machine_info.assert_called_once_with(self.owner_id, self.machine_id)


        """
        Test case for determining the next available version.

        Case: Various current and available version scenarios.
        Expected Result: The method returns the correct next version based on the provided 
        current and available versions, covering patch updates, minor updates, and cases 
        with no updates available.
        """
        test_cases = [
            ("1.0.0", ["1.0.0", "1.0.1", "1.0.2", "1.1.0", "2.0.0"], "1.0.2"),  # Patch update
            ("1.0.2", ["1.0.2", "1.1.0", "1.2.0", "2.0.0"], "1.1.0"),  # Minor update
            ("1.1.0", ["1.1.0", "1.1.1", "2.0.0"], "1.1.1"),  # Patch update when minor is available
            ("1.1.1", ["1.1.1", "2.0.0"], "1.1.1"),  # No higher updates
            ("1.0.0", [], "1.0.0"), # No available updates
            ("2.0.0", ["1.0.0", "1.0.1", "1.1.0", "1.2.0"], "2.0.0"), # Current update > available
        ]

        for current, available, expected in test_cases:
            with self.subTest(current=current, available=available, expected=expected):
                result = self.csa_updater_service._get_next_version(
                    version.parse(current),
                    [version.parse(v) for v in available]
                )
                self.assertEqual(str(result), expected)


    def test_get_next_version_patch_update(self):
        """
        Test case for determining the next version in a patch update scenario.

        Case: Current version is a patch version with newer patch versions available.
        Expected Result: The method returns the next patch version correctly.
        """
        # Mock
        current_version = version.parse("1.0.0")
        available_versions = [
            version.parse(v) for v in ["1.0.0", "1.0.1", "1.0.2", "1.1.0"]
        ]
        # Call Method
        next_version = self.csa_updater_service._get_next_version(
            current_version, available_versions
        )
        # Assertions
        self.assertEqual(str(next_version), "1.0.2")


    def test_get_next_version_minor_update(self):
        """
        Test case for determining the next version in a minor update scenario.

        Case: Current version is a patch version with a newer minor version available.
        Expected Result: The method returns the next minor version correctly.
        """
        # Mock
        current_version = version.parse("1.0.2")
        available_versions = [
            version.parse(v) for v in ["1.0.2", "1.1.0", "1.1.1", "2.0.0"]
        ]
        
        # Call Method
        next_version = self.csa_updater_service._get_next_version(
            current_version, available_versions
        )
        
        # Assertions
        self.assertEqual(str(next_version), "1.1.0")  


    def test_generate_asset_key_windows(self):
        """
        Test case for generating asset key for Windows platform.

        Case: Generating asset key for a module on Windows.
        Expected Result: The method returns the correctly formatted asset key for Windows.
        """
        # Mock
        module_name = "test_module"
        next_version = version.parse("1.0.1")
        platform = "windows"
        
        # Call Method
        key = self.csa_updater_service._generate_asset_key(
            module_name, next_version, platform
        )
        
        expected_key = f"system/csa_modules/{module_name}/{platform}/{module_name}.1.0.1.zip"

        # Assertions
        self.assertEqual(key, expected_key)


    def test_generate_asset_key_linux(self):
        """
        Test case for generating asset key for Linux platform.

        Case: Generating asset key for a module on Linux.
        Expected Result: The method returns the correctly formatted asset key for Linux.
        """
        # Mock
        module_name = "test_module"
        next_version = version.parse("1.0.1")
        platform = "linux"
        
        # Call Method
        key = self.csa_updater_service._generate_asset_key(
            module_name, next_version, platform
        )
        
        # Assertions
        expected_key = f"system/csa_modules/{module_name}/{platform}/{module_name}.1.0.1.tar"
        self.assertEqual(key, expected_key)


    def test_create_targets_item_success(self):
        """
        Test case for creating target items successfully.

        Case: Valid inputs for module information and asset generation.
        Expected Result: The method returns a target item with correct module name, version, 
        checksum, and pre-signed URL.
        """
        # Mock
        self.s3_service_mock.generate_download_pre_signed_url.return_value = self.FAKE_PRESIGNED_URL
            
        next_version = version.parse("1.0.1")
        module_infos = [ModuleInfo(module_name=self.module_name, version="1.0.1", checksum=self.FAKE_CHECKSUM)]
            
        # Call Method
        target_item = self.csa_updater_service._create_targets_item(
            self.module_name, next_version, module_infos, self.platform
        )

        # Assertions
        self.assertEqual(target_item.module_name, self.module_name)
        self.assertEqual(target_item.version, "1.0.1")
        self.assertEqual(target_item.presigned_url, self.FAKE_PRESIGNED_URL)
        self.assertEqual(target_item.checksum, self.FAKE_CHECKSUM)


    def test_create_targets_item_raises_service_exception(self):
        """
        Test case for handling missing version in target item creation.

        Case: The requested next version does not exist in module_infos.
        Expected Result: The method raises a ServiceException indicating no matching version found.
        """
        # Mock
        next_version = version.parse("1.0.2")  # Version that does not exist in module_infos
        module_infos = [ModuleInfo(module_name=self.module_name, version="1.0.1", checksum=self.FAKE_CHECKSUM)]

        # Call Method
        with self.assertRaises(ServiceException) as e:
            self.csa_updater_service._create_targets_item(
                self.module_name, next_version, module_infos, self.platform
            )

        # Assertions
        self.assertEqual(e.exception.status_code, 400)  
        self.assertEqual(e.exception.message, "No matching version found")  


    def test_update_modules_success(self):
        """
        Test case for successfully updating module versions.

        Case: A list of target updates is provided.
        Expected Result: The method calls the repository to update the modules with the correct 
        module names and versions.
        """
        # Mock
        targets = [
            Targets(module_name="module1", version="1.0.0", presigned_url="url1", checksum=self.FAKE_CHECKSUM),
            Targets(module_name="module2", version="2.0.0", presigned_url="url2", checksum=self.FAKE_CHECKSUM)
        ]
        # Call the method
        self.csa_updater_service._update_modules(self.machine_id, self.owner_id, targets)

        expected_update_list = [
            {"module_name": "module1", "version": "1.0.0"},
            {"module_name": "module2", "version": "2.0.0"}
        ]
        self.csa_machines_repo.update_modules.assert_called_once_with(
            self.owner_id, self.machine_id, expected_update_list
        )


    def test_update_modules_empty_targets(self):
        """
        Test case for handling empty targets list during module update.

        Case: The targets parameter is an empty list.
        Expected Result: The method raises a ServiceException indicating no targets found.
        """
        # Mock
        empty_targets = []

        # Call the method
        with self.assertRaises(ServiceException) as e:
            self.csa_updater_service._update_modules(self.machine_id, self.owner_id, empty_targets)

        # Assertions
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "No targets found")
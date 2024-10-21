from packaging import version
from typing import List, Dict
from configuration import S3AssetsFileConfig
from controller import common_controller as common_ctrl
from repository import CsaMachinesRepository, CsaModuleVersionsRepository
from model import Module, Targets, UpdateResponse, MachineInfo, ModuleInfo
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus
from service.s3_service.s3_assets_service import S3AssetsService


log = common_ctrl.log


class CsaUpdaterService(metaclass=Singleton):


    def __init__(self, csa_machines_repository: CsaMachinesRepository, csa_module_versions_repository: CsaModuleVersionsRepository, s3_assets_file_config: S3AssetsFileConfig) -> None:
        """
        Initializes the CsaUpdaterService with the provided repository and S3 asset configuration.

        Args:
            csa_module_versions_repository (CsaModuleVersionsRepository): The repository to interact with the CSA's modules.
            csa_machines_repository (CsaMachinesRepository): The repository to interact with the CSA machine info.
            s3_assets_file_config (S3AssetsFileConfig): Configuration for S3 asset file handling.
        """
        self.csa_machines_repository = csa_machines_repository
        self.csa_module_versions_repository = csa_module_versions_repository
        self.s3_asset_service = S3AssetsService(s3_assets_file_config)


    def get_targets(self, owner_id: str, machine_id: str, machine_modules: List[Module]) -> UpdateResponse:
        """
        Generates a target list of modules the machine needs based on the current machine's versions and repository data.

        Args:
            machine_id (str): The ID of the machine requesting updates.
            owner_id (str): The owner ID associated with the machine.
            machine_modules (List[Module]): The list of modules and their versions on the machine.

        Returns:
            UpdateResponse: The response containing the target list of modules.
        """
        if not machine_modules:
            log.error("No machine modules found. owner_id: %s, machine_id: %s", owner_id, machine_id)
            raise ServiceException(400, ServiceStatus.FAILURE, "No machine modules found.")
        
        log.info('Getting target list for owner_id: %s, machine_id: %s', owner_id, machine_id)
        
        csa_machine_info: MachineInfo = self.csa_machines_repository.get_csa_machine_info(owner_id, machine_id)
        
        machine_versions = self._get_machine_versions(machine_modules)
        
        targets = self._process_modules(csa_machine_info.modules, machine_versions, csa_machine_info.platform)
        
        self._update_modules(machine_id, owner_id, targets)
        
        log.info('Successfully got target list. owner_id: %s, machine_id: %s, targets: %s', owner_id, machine_id, targets)
        
        return UpdateResponse(targets=targets)


    def _get_machine_versions(self, machine_modules: List[Dict]) -> Dict[str, version.Version]:
        """
        Converts the machine_modules into a dictionary of module names and their versions.

        Args:
            machine_modules (List[Dict]): The list of dictionaries containing module information.
                Each dictionary should have 'module_name' and 'version' keys.

        Returns:
            Dict[str, version.Version]: A dictionary of module names and their parsed versions.
        """
        return {
            module['module_name']: version.parse(module['version']) 
            for module in machine_modules
        }


    def _process_modules(self, modules: List[Module], machine_versions: Dict[str, version.Version], platform: str) -> List[Targets]:
        """
        Processes the modules to determine which need updates and generates the target list.

        Args:
            modules (List[Module]): The list of modules retrieved from the CSA table.
            machine_versions (Dict[str, version.Version]): A dictionary mapping module names to their versions.
            platform (str): The platform (e.g., 'windows' or 'linux') for generating S3 keys.

        Returns:
            List[Targets]: The target list containing modules that require updates.
        """
        targets = []
        for module in modules:
            module_name = module.module_name
            current_version = version.parse(module.version)
            machine_version = machine_versions.get(module_name, current_version)

            module_infos = self.csa_module_versions_repository.get_csa_module_versions(module_name)
            
            available_versions = [version.parse(item.version) for item in module_infos]

            next_version = self._get_next_version(current_version, available_versions)

            if next_version.minor != machine_version.minor and next_version.minor > machine_version.minor:
                next_version = version.parse(f"{next_version.major}.{next_version.minor}.0")
            
            targets.append(self._create_targets_item(module_name, next_version, module_infos, platform))

        return targets  


    def _get_next_version(self, current_version: version.Version, available_versions: List[version.Version]) -> version.Version:
        """
        Determines the next version to update to based on the available versions and the current version.
        
        Args:
            current_version (version.Version): The current version of the module from the CSA machines table.
            available_versions (List[version.Version]): The list of available versions from the repository.

        Returns:
            version.Version: The next version to update to, or the current version if no updates are available.
        """
        next_patch, next_minor= None, None

        for version in available_versions:
            if version <= current_version:
                continue
            
            if version.major == current_version.major:
                if version.minor == current_version.minor:
                    next_patch = max(next_patch, version) if next_patch else version
                elif version.minor > current_version.minor:
                    next_minor = min(next_minor, version) if next_minor else version
        
        return next_patch or next_minor or current_version


    def _generate_asset_key(self, module_name: str, next_version: version.Version, platform: str) -> str:
        """
        Generates an S3 key for the module asset based on the platform and version.
        
        Args:
            module_name (str): Name of the module.
            next_version (version.Version): The next version of the module to be downloaded.
            platform (str): The platform (e.g., 'windows' or 'linux').
        
        Returns:
            str: The generated S3 key.
        
        Raises:
            ValueError: If the platform is unknown.
        """
        file_extension = '.zip' if platform == 'windows' else '.tar'
        
        return f"system/csa_modules/{module_name}/{platform}/{module_name}.{next_version}{file_extension}"


    def _create_targets_item(self, module_name: str, next_version: version.Version, module_infos: List[ModuleInfo], platform: str) -> Targets:
        """
        Creates a target list item for the module to be downloaded, including its presigned URL and checksum.

        Args:
            module_name (str): The name of the module.
            next_version (version.Version): The next version of the module to be downloaded.
            module_infos (List[ModuleInfo]): The module information including available versions.
            platform (str): The platform (e.g., 'windows' or 'linux') for generating the S3 key.

        Returns:
            Targets: The target list item for the specified module, version, and platform.
        """
        module_info_item = next((item for item in module_infos if item.version == str(next_version)), None)

        if not module_info_item:
            log.error("No matching version found for module_name: %s", module_name)
            raise ServiceException(400, ServiceStatus.FAILURE, "No matching version found")

        s3_key = self._generate_asset_key(module_name, next_version, platform)
        
        return Targets(
            module_name=module_name,
            version=str(next_version),
            presigned_url=self.s3_asset_service.generate_download_pre_signed_url(s3_key),
            checksum=module_info_item.checksum,
        )
    
    
    def _update_modules(self, machine_id: str, owner_id: str, targets: List[Targets]) -> None:
        """
        Updates the module version list in the csa machines repository with the new target list.

        Args:
            machine_id (str): The ID of the machine.
            owner_id (str): The owner ID associated with the machine.
            targets (List[Targets]): The list of modules with updates.
        """
        if not targets:
            log.error("No targets found. owner_id: %s, machine_id: %s", owner_id, machine_id)
            raise ServiceException(400, ServiceStatus.FAILURE, "No targets found")

        update_dependency_list = [
            {"module_name": item.module_name, "version": item.version} for item in targets
        ]
        self.csa_machines_repository.update_modules(owner_id, machine_id, update_dependency_list)

from packaging import version
from typing import List, Dict
from configuration import S3AssetsFileConfig
from controller import common_controller as common_ctrl
from repository import CsaUpdaterRepository
from model import Module, TargetList, UpdateResponse, MachineInfo, ModuleInfo
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus
from ..s3_service.s3_assets_service import S3AssetsService

log = common_ctrl.log

class CsaUpdaterService(metaclass=Singleton):


    def __init__(self, repository: CsaUpdaterRepository, s3_assets_file_config: S3AssetsFileConfig) -> None:
        """
        Initializes the CsaUpdaterService with the provided repository and S3 asset configuration.

        Args:
            repository (CsaUpdaterRepository): The repository to interact with the CSA data.
            s3_assets_file_config (S3AssetsFileConfig): Configuration for S3 asset file handling.
        """
        self.repository = repository
        self.s3_asset_service = S3AssetsService(s3_assets_file_config)


    def get_target_list(self, machine_id: str, owner_id: str, machine_info_list: List[Dict]) -> UpdateResponse:
        """
        Generates a target list of modules the machine needs based on the current machine's versions and repository data.

        Args:
            machine_id (str): The ID of the machine requesting updates.
            owner_id (str): The owner ID associated with the machine.
            machine_info_list (List[Module]): The list of modules and their versions on the machine.

        Returns:
            UpdateResponse: The response containing the target list of modules.
        """
        log.info('Getting target list for owner_id: %s and machine_id: %s', owner_id, machine_id)
        
        # Retrieve CSA table info
        csa_table_info: MachineInfo = self._get_csa_table_info(owner_id, machine_id)  # Ensure type hinting
        modules = csa_table_info.modules 
        platform = csa_table_info.platform  
        # Get machine versions
        machine_versions = self._get_machine_versions(machine_info_list)
        
        # Process modules and generate target list
        target_list = self._process_modules(modules, machine_versions, platform)
        
        # Update module version list in repository
        self._update_modules(machine_id, owner_id, target_list)
        
        log.info("Final target list: %s", target_list)
        return UpdateResponse(target_list=target_list)


    def _get_csa_table_info(self, owner_id: str, machine_id: str) -> Dict:
        """
        Retrieves information from the CSA table based on owner_id and machine_id.

        Args:
            owner_id (str): The owner ID.
            machine_id (str): The machine ID.

        Returns:
            Dict: The CSA table information.

        Raises:
            ServiceException: If no information is found for the machine_id.
        """
        csa_table_info = self.repository.get_csa_machines_info(owner_id=owner_id, machine_id=machine_id)
        if not csa_table_info:
            raise ServiceException(404, ServiceStatus.FAILURE, f"No target list found for machine_id: {machine_id}")
        return csa_table_info[0]


    def _get_machine_versions(self, machine_info_list: List[Dict]) -> Dict[str, version.Version]:
        """
        Converts the machine_info_list into a dictionary of module names and their versions.

        Args:
            machine_info_list (List[Dict]): The list of dictionaries containing module information.
                Each dictionary should have 'module_name' and 'version' keys.

        Returns:
            Dict[str, version.Version]: A dictionary of module names and their parsed versions.
        """
        log.debug(f"Parsing machine versions: {machine_info_list}")
        return {
            module['module_name']: version.parse(module['version']) 
            for module in machine_info_list
        }


    def _process_modules(self, modules: List[Module], machine_versions: Dict[str, version.Version], platform: str) -> List[TargetList]:
        """
        Processes the modules to determine which need updates and generates the target list.

        Args:
            modules (List[Module]): The list of modules retrieved from the CSA table.
            machine_versions (Dict[str, version.Version]): A dictionary mapping module names to their versions.
            platform (str): The platform (e.g., 'windows' or 'linux') for generating S3 keys.

        Returns:
            List[TargetList]: The target list containing modules that require updates.
        """
        target_list = []
        for module_version in modules:
            module_name = module_version.module_name
            current_version = version.parse(module_version.version)
            machine_version = machine_versions.get(module_name, current_version)

            log.debug(f"Processing module: {module_name}, Current version: {current_version}, Machine version: {machine_version}")

            release_info = self.repository.get_csa_module_versions(module_name)
            
            log.debug(f"Release info for {module_name}: {release_info}")
            
            if not release_info:
                log.warning(f"No release info found for module: {module_name}")
                continue

            available_versions = [version.parse(item.version) for item in release_info]

            log.info(f"Module: {module_name}, Current version: {current_version}, "
                    f"Machine version: {machine_version}, Available versions: {available_versions}")

            next_version = self._get_next_version(current_version, available_versions, machine_version)

            if next_version >= machine_version:
                target_list.append(self._create_target_list_item(module_name, next_version, release_info, platform))
                log.info(f"Update found for {module_name}: {machine_version} -> {next_version}")
            else:
                log.info(f"No update available for {module_name}. "
                        f"Machine version: {machine_version}, Current version: {current_version}, Next version: {next_version}")

        return target_list


    def _get_next_version(self, current_version: version.Version, available_versions: List[version.Version], machine_version: version.Version) -> version.Version:
        """
        Determines the next version to update to based on the available versions, the current version, and the machine's version.

        Args:
            current_version (version.Version): The current version of the module from the CSA machines table.
            available_versions (List[version.Version]): The list of available versions from the repository.
            machine_version (version.Version): The current version of the module on the machine.

        Returns:
            version.Version: The next version to update to, or the current version if no updates are available.
        """
        # Sort available versions
        sorted_versions = sorted(available_versions)
        
        try:
            # Find the index of the machine version
            machine_index = sorted_versions.index(machine_version)
            
            # Check for versions newer than the machine version up to and including the current version
            catchup_versions = [
                v for v in sorted_versions[machine_index + 1:]
                if v <= current_version and v.major == current_version.major and v.minor == current_version.minor
            ]
            
            if catchup_versions:
                return catchup_versions[-1]  # Return the latest micro version
        except ValueError:
            # If machine_version is not in available_versions, treat as new installation
            return current_version
        
        try:
            # If no catchup needed, proceed with the original logic
            current_index = sorted_versions.index(current_version)
            
            # Check for newer versions in the same minor series
            same_minor_versions = [
                v for v in sorted_versions[current_index + 1:]
                if v.major == current_version.major and v.minor == current_version.minor
            ]
            
            if same_minor_versions:
                return same_minor_versions[-1]  # Return the latest micro version
            
            # If no newer versions in the same minor series, look for the next minor version
            next_minor_versions = [
                v for v in sorted_versions[current_index + 1:]
                if v.major == current_version.major and v.minor == current_version.minor + 1
            ]
            
            if next_minor_versions:
                return next_minor_versions[0]  # Return the first version of the next minor
        except ValueError:
            # If current_version is not in available_versions
            pass
        # If no next minor version, return the current version (no update available)
        return current_version


    def _generate_s3_key(self, module_name: str, next_version: version.Version, platform: str) -> str:
        """
        Generates an S3 key for the module asset based on the platform and version.
        
        Args:
            module_name (str): Name of the module.
            next_version (version.Version): The next version of the module to be downloaded.
            platform (str): The platform (e.g., 'windows' or 'linux').
        
        Returns:
            str: The generated S3 key.
        """
        file_extension = '.zip' if platform == 'windows' else '.tar'
        return f"assets/system/csa_modules/{module_name}/{platform}/{module_name}.{next_version}{file_extension}"


    def _create_target_list_item(self, module_name: str, next_version: version.Version, 
                            release_info: List[ModuleInfo], platform: str) -> TargetList:
        """
        Creates a target list item for the module to be downloaded, including its presigned URL and checksum.

        Args:
            module_name (str): The name of the module.
            next_version (version.Version): The next version of the module to be downloaded.
            release_info (List[ModuleInfo]): The module information including available versions.
            platform (str): The platform (e.g., 'windows' or 'linux') for generating the S3 key.

        Returns:
            TargetList: The target list item for the specified module, version, and platform.
        """
        release_info_item = next(item for item in release_info if item.version == str(next_version))
        s3_key = self._generate_s3_key(module_name, next_version, platform)
        
        return TargetList(
            module_name=module_name,
            version=str(next_version),
            presigned_url=self.s3_asset_service.generate_download_pre_signed_url(s3_key),
            checksum=release_info_item.checksum,
        )
    
    
    def _update_modules(self, machine_id: str, owner_id: str, target_list: List[TargetList]) -> None:
        """
        Updates the module version list in the repository with the new target list.

        Args:
            machine_id (str): The ID of the machine.
            owner_id (str): The owner ID associated with the machine.
            target_list (List[TargetList]): The list of modules with updates.
        """
        update_dependency_list = [
            {"module_name": item.module_name, "version": item.version} for item in target_list
        ]
        self.repository.update_modules(owner_id, machine_id, update_dependency_list)

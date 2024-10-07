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


    def get_target_list(self, owner_id: str, machine_id: str, machine_modules: List[Module]) -> UpdateResponse:
        """
        Generates a target list of modules the machine needs based on the current machine's versions and repository data.

        Args:
            machine_id (str): The ID of the machine requesting updates.
            owner_id (str): The owner ID associated with the machine.
            machine_modules (List[Module]): The list of modules and their versions on the machine.

        Returns:
            UpdateResponse: The response containing the target list of modules.
        """
        log.info('Getting target list for owner_id: %s and machine_id: %s', owner_id, machine_id)
        
        csa_table_info: MachineInfo = self._get_csa_machine_info(owner_id, machine_id) 
        modules = csa_table_info.modules 
        platform = csa_table_info.platform 

        machine_versions = self._get_machine_versions(machine_modules)
        
        target_list = self._process_modules(modules, machine_versions, platform)
        
        self._update_modules(machine_id, owner_id, target_list)
        
        log.info("Final target list: %s", target_list)
        return UpdateResponse(target_list=target_list)


    def _get_csa_machine_info(self, owner_id: str, machine_id: str) -> Dict:
        """
        Retrieves information from the CSA machine based on owner_id and machine_id.

        Args:
            owner_id (str): The owner ID.
            machine_id (str): The machine ID.

        Returns:
            Dict: The CSA machine information.

        Raises:
            ServiceException: If no information is found for the machine_id.
        """
        csa_table_info = self.repository.get_csa_machines_info(owner_id=owner_id, machine_id=machine_id)
        if not csa_table_info:
            raise ServiceException(404, ServiceStatus.FAILURE, f"No target list found for machine_id: {machine_id}")
        return csa_table_info[0]


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

            module_version_info = self.repository.get_csa_module_versions(module_name)
            
            if not module_version_info:
                log.warning(f"No release info found for module: {module_name}")
                continue

            available_versions = [version.parse(item.version) for item in module_version_info]

            log.info(f"Module: {module_name}, Current version: {current_version}, "
                    f"Machine version: {machine_version}, Available versions: {available_versions}")

            next_version = self._get_next_version(current_version, available_versions)

            if next_version.minor != machine_version.minor and next_version.minor > machine_version.minor:
                next_version_string = version.parse(f"{next_version.major}.{next_version.minor}.0")
                target_list.append(self._create_target_list_item(module_name, next_version_string, module_version_info, platform))
                log.info(f"Update found for {module_name}: {machine_version} -> {next_version_string}")
            else:
                target_list.append(self._create_target_list_item(module_name, next_version, module_version_info, platform))
                log.info(f"Update found for {module_name}: {machine_version} -> {next_version}")

        return target_list


    def _get_next_version(self, current_version: version.Version, available_versions: List[version.Version]) -> version.Version:
        """
        Determines the next version to update to based on the available versions and the current version.
        
        Args:
            current_version (version.Version): The current version of the module from the CSA machines table.
            available_versions (List[version.Version]): The list of available versions from the repository.

        Returns:
            version.Version: The next version to update to, or the current version if no updates are available.
        """
        next_minor_version = None
        next_patch_version = None

        for available_version in available_versions:
            if available_version <= current_version:
                continue  # Skip versions that are older or equal to the current version
            
            # Check for the next patch version (same major.minor but higher patch)
            if (available_version.major == current_version.major and 
                available_version.minor == current_version.minor and 
                (next_patch_version is None or available_version > next_patch_version)):
                next_patch_version = available_version
            
            # Check for the next minor version (higher minor)
            if (available_version.major == current_version.major and 
                available_version.minor > current_version.minor and 
                (next_minor_version is None or available_version < next_minor_version)):
                next_minor_version = available_version

        # If there is a higher patch version, return that, otherwise return the next minor version.
        return next_patch_version or next_minor_version or current_version


    def _generate_asset_key(self, module_name: str, next_version: version.Version, platform: str) -> str:
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
        return f"system/csa_modules/{module_name}/{platform}/{module_name}.{next_version}{file_extension}"


    def _create_target_list_item(self, module_name: str, next_version: version.Version, 
                            module_version_info: List[ModuleInfo], platform: str) -> TargetList:
        """
        Creates a target list item for the module to be downloaded, including its presigned URL and checksum.

        Args:
            module_name (str): The name of the module.
            next_version (version.Version): The next version of the module to be downloaded.
            module_version_info (List[ModuleInfo]): The module information including available versions.
            platform (str): The platform (e.g., 'windows' or 'linux') for generating the S3 key.

        Returns:
            TargetList: The target list item for the specified module, version, and platform.
        """
        try:
            module_info_item = next(item for item in module_version_info if item.version == str(next_version))
        except StopIteration:
            log.error(f"No matching version found for {module_name} with version {next_version}")
        s3_key = self._generate_asset_key(module_name, next_version, platform)
        
        return TargetList(
            module_name=module_name,
            version=str(next_version),
            presigned_url=self.s3_asset_service.generate_download_pre_signed_url(s3_key),
            checksum=module_info_item.checksum,
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

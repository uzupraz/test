from botocore.exceptions import ClientError
from packaging import version
from typing import List, Dict, Optional
from configuration import S3AssetsFileConfig
from controller import common_controller as common_ctrl
from repository import UpdaterRepository
from model import Module, TargetList, UpdateResponse
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus, SystemStatus
from .s3_service.s3_assets_service import S3AssetsService

log = common_ctrl.log

class UpdaterService(metaclass=Singleton):
    def __init__(self, repository: UpdaterRepository, s3_assets_file_config: S3AssetsFileConfig) -> None:
        self.repository = repository
        self.s3_asset_service = S3AssetsService(s3_assets_file_config)

    def get_target_list(self, machine_id: str, owner_id: str, machine_info_list: List[Module]) -> UpdateResponse:
        log.info(f'Getting target_list. owner_id: {owner_id}, machine_id: {machine_id}')
        
        csa_table_info = self._get_csa_table_info(owner_id, machine_id)
        module_version_list = csa_table_info.get('module_version_list', [])
        platform = csa_table_info.get('platform', 'windows')
        
        machine_versions = self._get_machine_versions(machine_info_list)
        log.info(f"Machine versions: {machine_versions}")
        log.info(f"Module version list from CSA table: {module_version_list}")

        target_list = self._process_modules(module_version_list, machine_versions, platform)
        
        self._update_module_version_list(machine_id, owner_id, target_list)
        
        log.info(f"Final target list: {target_list}")
        return UpdateResponse(target_list=target_list)

    def _get_csa_table_info(self, owner_id: str, machine_id: str) -> Dict:
        csa_table_info = self.repository.get_csa_info_table_items(owner_id=owner_id, machine_id=machine_id)
        if not csa_table_info:
            raise ServiceException(404, ServiceStatus.FAILURE, f"No target list found for machine_id: {machine_id}")
        return csa_table_info[0]

    def _get_machine_versions(self, machine_info_list: List[Module]) -> Dict[str, version.Version]:
        return {machine_list['module_name']: version.parse(machine_list['version']) for machine_list in machine_info_list}

    def _process_modules(self, module_version_list: List[Dict], machine_versions: Dict[str, version.Version], platform: str) -> List[TargetList]:
        target_list = []
        for module_version in module_version_list:
            module_name = module_version['module_name']
            current_version = version.parse(module_version['version'])
            machine_version = machine_versions.get(module_name, current_version)
            
            release_info = self.repository.get_release_info(module_name)
            available_versions = [version.parse(item['version']) for item in release_info]
            
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
        sorted_available_versions = sorted(available_versions, reverse=True)
        
        if machine_version <= current_version:
            next_minor = version.parse(f"{machine_version.major}.{machine_version.minor + 1}.0")
            for av in sorted_available_versions:
                if av <= next_minor and av > machine_version:
                    return av
            return machine_version
        
        next_minor = version.parse(f"{current_version.major}.{current_version.minor + 1}.0")
        for av in sorted_available_versions:
            if av <= next_minor and av > current_version:
                return av
        
        return current_version

    def _create_target_list_item(self, module_name: str, next_version: version.Version, release_info: List[Dict], platform: str) -> TargetList:
        release_info_item = next(item for item in release_info if item['version'] == str(next_version))
        file_extension = '.zip' if platform == 'windows' else '.tar'
        s3_key = f"system/csa_modules/{module_name}/{platform}/{module_name}.{next_version}{file_extension}"
        
        return TargetList(
            module_name=module_name,
            version=str(next_version),
            presigned_url=self.s3_asset_service.get_updates_url_from_s3(s3_key),
            checksum=release_info_item['checksum']
        )

    def _update_module_version_list(self, machine_id: str, owner_id: str, target_list: List[TargetList]) -> None:
        update_module_version_list = [{'module_name': dep.module_name, 'version': dep.version} for dep in target_list]
        self.repository.update_module_version_list(machine_id, owner_id, update_module_version_list)
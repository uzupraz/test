import nanoid

from typing import List, Union, Dict, Any, Optional
from dataclasses import asdict
from dacite import from_dict

from exception import ServiceException
from enums import ServiceStatus
from controller import common_controller as common_ctrl
from utils import Singleton
from model import CustomScript, CustomScriptUnpublishedChange, CustomScriptRelease, CustomScriptMetadata, CustomScriptRequestDTO, UnpublishedChangeResponseDTO, CustomScriptContentResponse
from repository import CustomScriptRepository
from .s3_service.s3_assets_service import S3AssetsService

log = common_ctrl.log

class CustomScriptService(metaclass=Singleton):


    def __init__(self, s3_assets_service: S3AssetsService, custom_script_repository: CustomScriptRepository) -> None:
        """
        Initializes the CustomScriptService with the CustomScriptRepository and S3AssetsService.

        Args:
            s3_assets_service (S3AssetsService): The repository instance to access assets s3 methods.
            custom_script_repository (CustomScriptRepository): The repository instance to access assets methods.
        """
        self.custom_script_repository = custom_script_repository
        self.s3_assets_service = s3_assets_service


    def save_custom_script(self, owner_id: str, payload: CustomScriptRequestDTO) -> Dict[str, Any]:
        """
        Save the content of a custom script to S3 and update its unpublished changes in the repository.

        Args:
            owner_id (str): The ID of the owner of the script.
            payload (CustomScriptRequestDTO): Save custom script payload

        Updates:
            The unpublished changes for the script with the new version ID.
        """
        log.info('Saving custom script. owner_id: %s', owner_id)

        # Fetch or create a custom script
        custom_script = self._get_or_create_custom_script(owner_id, payload)

        # Get source version ID (either from unpublished changes or the provided payload)
        source_version_id = self._determine_source_version_id(owner_id, custom_script, payload)

        # Upload the script to S3 and get the new version ID
        version_id = self._upload_script_to_s3(owner_id, custom_script, payload.script)

        # Create and merge unpublished changes
        unpublished_change = CustomScriptUnpublishedChange(
            version_id=version_id,
            edited_by=owner_id,
            source_version_id=source_version_id,
        )
        unpublished_changes = self._merge_unpublished_changes(unpublished_change, custom_script.unpublished_changes)

        # Update the repository with the new unpublished changes
        self.custom_script_repository.update_unpublished_changes(owner_id, custom_script.script_id, unpublished_changes)

        # Construct and return the response
        response = asdict(unpublished_changes[-1])
        response['script_id'] = custom_script.script_id
        return from_dict(UnpublishedChangeResponseDTO, response)
    

    def get_custom_scripts(self, owner_id: str) -> List[CustomScript]:
        """
        Retrieve all custom scripts owned by the specified owner, updating them with the owner's unpublished changes.

        Args:
            owner_id (str): The ID of the owner whose scripts are being retrieved.

        Returns:
            List[CustomScript]: A list of custom scripts with updated unpublished changes.
        """
        log.info('Retrieving owner custom scripts. owner_id: %s', owner_id)
        custom_scripts = self.custom_script_repository.get_owner_custom_scripts(owner_id)
        updated_scripts = []
        for script in custom_scripts:
            changes = self._get_owner_unpublished_change(owner_id, script.unpublished_changes)
            script.unpublished_changes = [changes] if changes else []
            updated_scripts.append(script)
        
        return updated_scripts


    def get_custom_script_content(self, owner_id: str, script_id: str, branch: str, version_id: Union[str, None]) -> str:
        """
        Retrieve the content of a specific custom script from S3.

        Args:
            owner_id (str): The ID of the owner of the script.
            script_id (str): The ID of the script being retrieved.
            branch (str): The branch name either release or unpublished
            version_id (Union[str, None]): Get specific version

        Returns:
            str: The content of the custom script, retrieved from S3 using the version ID from the unpublished changes.
        """
        log.info('Retrieving custom script content. owner_id: %s, script_id: %s', owner_id, script_id)

        from_release = branch == 'release'
        custom_script = self.custom_script_repository.get_custom_script(owner_id, script_id)

        if from_release and not custom_script.releases:
            log.error('No releases found. owner_id: %s, script_id: %s', owner_id, script_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'No releases found')

        if from_release and version_id:
            release = next((release for release in custom_script.releases if release.version_id == version_id), None)
            if not release:
                log.error('Release not found for provided version id. owner_id: %s, script_id: %s, version_id: %s', owner_id, script_id, version_id)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Release not found for provided version id')
            
        if not from_release and not self._get_owner_unpublished_change(owner_id, custom_script.unpublished_changes):
            log.error('Unpublished changes not available. owner_id: %s, script_id: %s', owner_id, script_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Unpublished changes not available')
        
        key = self._generate_relative_path(owner_id, custom_script.script_id, custom_script.extension, from_release)
        return CustomScriptContentResponse(content=self.s3_assets_service.get_script_from_s3(owner_id=owner_id, relative_path=key, version_id=version_id))


    def release_custom_script(self, owner_id: str, script_id: str):
        """
        Release the latest unpublished changes of a custom script by publishing the script to S3.

        Args:
            owner_id (str): The ID of the owner of the script.
            script_id (str): The ID of the script being released.

        Raises:
            ServiceException: If no unpublished changes are found for the script.

        Updates:
            The list of releases for the custom script with the newly published version.
        """
        log.info('Retrieving customer tables. owner_id: %s', owner_id)
        custom_script = self.custom_script_repository.get_custom_script(owner_id, script_id)
        
        # Getting owners unpublished changes
        unpublished_change = self._get_owner_unpublished_change(owner_id, custom_script.unpublished_changes)
        if not unpublished_change:
            log.error('Unpublished change not found. owner_id: %s, script_id: %s', owner_id, script_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Unpublished change not found.')

        # Constructing and getting unpublished changes from s3
        relative_path = self._generate_relative_path(owner_id, script_id, custom_script.extension)
        content = self.s3_assets_service.get_script_from_s3(owner_id, relative_path, unpublished_change.version_id)

        # Releasing changes
        version_id = self._upload_script_to_s3(owner_id, custom_script, content, True)
        release = CustomScriptRelease(version_id=version_id, edited_by=owner_id, source_version_id=unpublished_change.version_id)

        # Updating releases
        custom_script.releases.append(release)
        self.custom_script_repository.update_releases(owner_id, script_id, custom_script.releases)
        
        # Removing unpublished changes from the list
        filtered_unpublished_changes = []
        for script in custom_script.unpublished_changes:
            if script.edited_by == owner_id:
                continue
            filtered_unpublished_changes.append(script)
        self.custom_script_repository.update_unpublished_changes(owner_id, script_id, filtered_unpublished_changes)

        return release
    

    def remove_unpublished_change(self, owner_id: str, script_id: str) -> None:
        """
        Removes unpublished changes from database only

        Args:
            owner_id (str): The ID of the owner of the script.
            script_id (str): The ID of the script being released.
        """
        custom_script = self.custom_script_repository.get_custom_script(owner_id, script_id)
        custom_script.unpublished_changes = [change for change in custom_script.unpublished_changes if change.edited_by != owner_id]
        self.custom_script_repository.update_unpublished_changes(owner_id, script_id, custom_script.unpublished_changes)


    # Helper Methods
    def _merge_unpublished_changes(self, unpublished_change: CustomScriptUnpublishedChange, existing_changes: List[CustomScriptUnpublishedChange]):
        """
        Update the list of unpublished changes with a new change.
        
        This method checks if the owner already has unpublished changes. If so, it updates the existing
        change with the new one; otherwise, it appends the new unpublished change to the list.

        Args:
            unpublished_change (CustomScriptUnpublishedChange): The new unpublished change to merge.
            existing_changes (List[CustomScriptUnpublishedChange]): List of current unpublished changes.
        
        Returns:
            List[CustomScriptUnpublishedChange]: The updated list of unpublished changes.
        """
        latest_change = self._get_owner_unpublished_change(unpublished_change.edited_by, existing_changes)

        if not latest_change:
            existing_changes.append(unpublished_change)
            return existing_changes
        
        unpublished_changes = [
            item if item.edited_by != unpublished_change.edited_by else unpublished_change
            for item in existing_changes
        ]
        return unpublished_changes
    

    def _get_owner_unpublished_change(self, owner_id: str, unpublished_changes: List[CustomScriptUnpublishedChange]) -> Union[CustomScriptUnpublishedChange, None]:
        """
        Retrieve the unpublished changes made by the specified owner.

        This helper method scans through the list of unpublished changes and returns the change
        associated with the given owner ID, if it exists.

        Args:
            owner_id (str): The ID of the owner who made the changes.
            unpublished_changes (List[CustomScriptUnpublishedChange]): List of all unpublished changes.
        
        Returns:
            Union[CustomScriptUnpublishedChange, None]: The unpublished change for the given owner,
            or None if no changes exist.
        """
        return next((change for change in unpublished_changes if change.edited_by == owner_id), None)
    

    def _get_release_by_version_id(self, version_id: str, releases: List[CustomScriptRelease]) -> Union[CustomScriptRelease, None]:
        """
        Get the release that matches the given version ID from the list of releases.

        This method searches through the list of releases and returns the release that has the
        specified version ID, if found.

        Args:
            version_id (str): The version ID to search for.
            releases (List[CustomScriptRelease]): List of all script releases.

        Returns:
            Union[CustomScriptRelease, None]: The matching release, or None if no release matches the version ID.
        """
        return next((release for release in releases if release.version_id == version_id), None)
    

    def _generate_relative_path(self, owner_id: str, script_id: str, extension: str, for_release: bool = False) -> str:
        """
        Generate a relative path for storing the script in S3.

        This method constructs a relative path based on the owner ID, script ID, and file extension.
        If the script is being uploaded for release, it omits the owner ID prefix.

        Args:
            owner_id (str): The ID of the owner of the script.
            script_id (str): The unique ID of the script.
            extension (str): The file extension of the script (e.g., 'js', 'py').
            for_release (bool): Indicates if the path is for a released script. Default is False.

        Returns:
            str: The relative path to be used in S3 for storing the script.
        """
        prefix = f"{owner_id}_" if not for_release else ""
        return f"{script_id}/{prefix}{script_id}.{extension}"
    

    def _get_or_create_custom_script(self, owner_id: str, payload: CustomScriptRequestDTO) -> CustomScript:
        """
        Fetches or creates a custom script based on the payload metadata.

        If metadata is provided in the payload, a new custom script is created with a generated script ID.
        If the script ID exists in the payload, the method retrieves the corresponding custom script from the repository.

        Args:
            owner_id (str): The ID of the owner of the custom script.
            payload (CustomScriptRequestDTO): The payload containing metadata or script ID.

        Returns:
            CustomScript: The custom script, either newly created or retrieved from the repository.
        """
        if payload.metadata:
            custom_script = CustomScript(
                owner_id=owner_id,
                script_id=nanoid.generate(),
                language=payload.metadata.language,
                name=payload.metadata.name,
                extension=payload.metadata.extension,
                releases=[],
                unpublished_changes=[]
            )
            self.custom_script_repository.create_custom_script(item=custom_script)
        else:
            custom_script = self.custom_script_repository.get_custom_script(owner_id, payload.script_id)
        return custom_script


    def _determine_source_version_id(self, owner_id: str, custom_script: CustomScript, payload: CustomScriptRequestDTO) -> Optional[str]:
        """
        Determines the source version ID for saving the custom script.

        This method checks if there are any unpublished changes by the owner. If so, it returns the
        source version ID from the unpublished changes. Otherwise, it looks for the source version
        ID in the payload and retrieves the corresponding release.

        Args:
            owner_id (str): The ID of the owner of the script.
            custom_script (CustomScript): The custom script object.
            payload (CustomScriptRequestDTO): The payload containing the source version ID.

        Returns:
            Optional[str]: The source version ID, or None if no valid source is found.
        """
        unpublished_change = self._get_owner_unpublished_change(owner_id, custom_script.unpublished_changes)
        
        if unpublished_change:
            return unpublished_change.source_version_id
        
        if payload.source_version_id:
            release = self._get_release_by_version_id(payload.source_version_id, custom_script.releases)
            if not release:
                log.error('Release source not found. owner_id: %s, script_id: %s', owner_id, custom_script.script_id)
                raise ServiceException(400, ServiceStatus.FAILURE, 'Release source not found')
            
            return release.version_id
        
        return None


    def _upload_script_to_s3(self, owner_id: str, custom_script: CustomScript, script_data: str, for_release: bool = None) -> str:
        """
        Uploads the script to S3 and returns the new version ID.

        This method generates the relative path for storing the script, uploads the script data to S3,
        and returns the version ID generated by S3.

        Args:
            owner_id (str): The ID of the owner of the script.
            custom_script (CustomScript): The custom script being uploaded.
            script_data (str): The content of the script to be uploaded.
            for_release (bool): Indicates if this is a script for release. Default is None.

        Returns:
            str: The version ID of the script uploaded to S3.
        """
        relative_path = self._generate_relative_path(owner_id, custom_script.script_id, custom_script.extension, for_release)
        return self.s3_assets_service.upload_script_to_s3(owner_id=owner_id, relative_path=relative_path, data=script_data)


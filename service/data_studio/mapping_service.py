import nanoid

from typing import List
from dacite import from_dict

from controller import common_controller as common_ctrl
from repository import DataStudioMappingRepository
from utils import Singleton
from model import User, DataStudioMapping, DataStudioMappingResponse, DataStudioSaveMapping
from exception import ServiceException
from enums import ServiceStatus, DataStudioMappingStatus

log = common_ctrl.log


class DataStudioMappingService(metaclass=Singleton):


    def __init__(self, data_studio_mapping_repository: DataStudioMappingRepository) -> None:
        self.data_studio_mapping_repository = data_studio_mapping_repository


    def get_active_mappings(self, owner_id:str) -> List[DataStudioMapping]:
        """
        Returns a list of active mappings for the given owner.
        Args:
            owner_id (str): The owner ID for which the active mappings are to be returned.
        Returns:
            list[DataStudioMapping]: List of active mappings for the given owner.
        """
        print(owner_id)
        return self.data_studio_mapping_repository.get_active_mappings(owner_id)


    def get_mapping(self, owner_id:str, user_id: str, mapping_id: str) -> DataStudioMappingResponse:
        """
        Returns revisions & draft for the given owner & mapping.
        Note: There is only one draft per user.
        Args:
            owner_id (str): The owner ID for which the mappings are to be returned.
            mapping_id (str): The mapping ID.
        Returns:
            DataStudioMappingResponse: Draft & Revisions of mapping for the given owner & mapping.
        """
        mappings = self.data_studio_mapping_repository.get_mapping(owner_id, mapping_id)

        draft = None
        revisions = []
        for mapping in mappings:
            if mapping.revision == user_id and mapping.status == DataStudioMappingStatus.DRAFT.value:
                draft = mapping
            elif mapping.status == DataStudioMappingStatus.PUBLISHED.value:
                revisions.append(mapping)

        return from_dict(DataStudioMappingResponse, {"draft": draft, "revisions": revisions})


    def create_mapping(self, user_id: str, owner_id: str) -> DataStudioMapping:
        """
        Creates a new data studio mapping and stores it in the database with partial values.

        Args:
            user_id (str): The ID of the user creating the mapping.
            owner_id (str): The ID of the owner associated with the mapping.

        Returns:
            DataStudioMapping: The newly created mapping.
        """
        mapping = DataStudioMapping(
            owner_id=owner_id,
            id=nanoid.generate(),
            revision=user_id,
            created_by=user_id,
            active=True
            )
        self.data_studio_mapping_repository.create_mapping(mapping)
        return mapping


    def save_mapping(self, user: User, mapping_dto: DataStudioSaveMapping) -> None:
        """
        Updates the draft mapping for a user if it exists.

        Args:
            user (str): User model.
            mapping (DataStudioSaveMapping): Mapping data to save.

        Raises:
            ServiceException: If the draft is not found or update fails.
        """
        draft_mapping = self.data_studio_mapping_repository.get_user_draft(user.organization_id, mapping_dto.id, user.sub)
        if not draft_mapping:
            log.error("Unable to find draft. owner_id: %s, user_id: %s, mapping_id: %s", user.organization_id, user.sub, mapping_dto.id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Unable to find draft.')

        # Updating changable fields
        draft_mapping.name = mapping_dto.name
        draft_mapping.description = mapping_dto.description
        draft_mapping.sources = mapping_dto.sources
        draft_mapping.output = mapping_dto.output
        draft_mapping.mapping = mapping_dto.mapping
        draft_mapping.tags = mapping_dto.tags

        self.data_studio_mapping_repository.save_mapping(owner_id=user.organization_id, revision=user.sub, mapping=draft_mapping)


    def publish_mapping(self, user_id: str, owner_id: str, mapping_id: str) -> DataStudioMapping:
        """
        Publishes a mapping draft if found.

        Args:
            user_id (str): The ID of the user performing the action.
            owner_id (str): The ID of the mapping owner.
            mapping_id (str): The ID of the mapping to publish.

        Returns:
            DataStudioMapping: The published mapping.

        Raises:
            ServiceException: If mapping draft not found or publication fails
        """
        draft_mapping = self.data_studio_mapping_repository.get_user_draft(owner_id, mapping_id, user_id)
        if not draft_mapping:
            log.error("Unable to find draft. owner_id: %s, user_id: %s, mapping_id: %s", owner_id, user_id, mapping_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Unable to find draft.')

        return self.data_studio_mapping_repository.publish_mapping(owner_id, user_id, draft_mapping)
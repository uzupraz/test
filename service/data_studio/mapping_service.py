import nanoid

from typing import List
from dacite import from_dict

from repository import DataStudioMappingRepository
from utils import Singleton, DataTypeUtils
from model import DataStudioMapping
from enums import DataStudioMappingStatus


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
        return self.data_studio_mapping_repository.get_active_mappings(owner_id)


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

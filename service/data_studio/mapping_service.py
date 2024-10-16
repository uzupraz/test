from typing import List
from dacite import from_dict

from repository import DataStudioMappingRepository
from utils import Singleton, DataTypeUtils
from model import DataStudioMapping


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
        mappings = self.data_studio_mapping_repository.get_active_mappings(owner_id)
        return [
            from_dict(DataStudioMapping, DataTypeUtils.convert_decimals_to_float_or_int(item)) 
            for item in mappings
        ]

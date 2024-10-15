from typing import List
from dacite import from_dict

from model import Workflow
from repository import WorkflowRepository, DataStudioMappingRepository
from utils import Singleton, DataTypeUtils
from model import DataStudioMapping


class DataStudioService(metaclass=Singleton):


    def __init__(self, workflow_repository: WorkflowRepository, data_studio_mapping_repository: DataStudioMappingRepository) -> None:
        self.workflow_repository = workflow_repository
        self.data_studio_mapping_repository = data_studio_mapping_repository


    def get_mappings(self, owner_id:str) -> List[DataStudioMapping]:
        """
        Returns a list of mappings for the given owner.
        Args:
            owner_id (str): The owner ID for which the mappings are to be returned.
        Returns:
            list[DataStudioMapping]: List of mappings for the given owner.
        """
        mappings = self.data_studio_mapping_repository.get_mappings(owner_id)
        return [
            from_dict(DataStudioMapping, DataTypeUtils.convert_decimals_to_float_or_int(item)) 
            for item in mappings
        ]


    def get_workflows(self, owner_id:str) -> list[Workflow]:
        """
        Returns a list of workflows for the given owner where the mapping_id is present.
        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.
        Returns:
            list[Workflow]: List of workflows for the given owner.
        """
        workflows_response = self.workflow_repository.get_data_studio_workflows(owner_id)
        workflows = [
            Workflow.from_dict(workflow_response)
            for workflow_response in workflows_response
        ]
        return workflows

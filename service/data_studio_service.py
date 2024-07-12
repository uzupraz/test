from model import DataStudioWorkflow
from repository import WorkflowRepository
from utils import Singleton


class DataStudioService(metaclass=Singleton):


    def __init__(self, workflow_repository: WorkflowRepository) -> None:
        self.workflow_repository = workflow_repository


    def get_mappings(self, owner_id:str) -> list:
        """
        Returns a list of mappings for the given owner.
        Args:
            owner_id (str): The owner ID for which the mappings are to be returned.
        Returns:
            list[Mapping]: List of mappings for the given owner.
        """
        return []


    def get_workflows(self, owner_id:str) -> list[DataStudioWorkflow]:
        """
        Returns a list of workflows for the given owner.
        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.
        Returns:
            list[DataStudioWorkflow]: List of workflows for the given owner.
        """
        workflows_response = self.workflow_repository.get_data_studio_workflows(owner_id)
        workflows = [
            DataStudioWorkflow.from_dict(workflow_response)
            for workflow_response in workflows_response
        ]
        return workflows

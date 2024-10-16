from model import Workflow
from repository import WorkflowRepository
from utils import Singleton


class DataStudioWorkflowService(metaclass=Singleton):


    def __init__(self, workflow_repository: WorkflowRepository) -> None:
        self.workflow_repository = workflow_repository


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

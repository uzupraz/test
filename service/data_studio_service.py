from model import DataStudioWorkflow
from repository import WorkflowRepository
from utils import Singleton


class DataStudioService(metaclass=Singleton):


    def __init__(self, workflow_repository: WorkflowRepository) -> None:
        self.workflow_repository = workflow_repository


    def get_mappings(self, owner_id: str) -> list:
        """
        Returns a list of mappings for the given owner.
        Args:
            owner_id (str): The owner ID for which the mappings are to be returned.
        Returns:
            list[Mapping]: List of mappings for the given owner.
        """
        return []


    def get_workflows(self, owner_id: str) -> list[DataStudioWorkflow]:
        """
        Returns a list of workflows for the given owner.
        Args:
            owner_id (str): The owner ID for which the workflows are to be returned.
        Returns:
            list[DataStudioWorkflow]: List of workflows for the given owner.
        """
        workflows_response = self.workflow_repository.find_datastudio_workflows(owner_id)
        workflows = [
            DataStudioWorkflow(
                owner_id=workflow_response.get('ownerId'),
                workflow_id=workflow_response.get('workflowId'),
                event_name=workflow_response.get('event_name'),
                created_by=workflow_response.get('createdBy'),
                created_by_name=workflow_response.get('createdByName'),
                last_updated=workflow_response.get('lastUpdated'),
                state=workflow_response.get('state'),
                version=workflow_response.get('version'),
                is_sync_execution=workflow_response.get('is_sync_execution'),
                state_machine_arn=workflow_response.get('state_machine_arn'),
                is_binary_event=workflow_response.get('is_binary_event'),
                mapping_id=workflow_response.get('mapping_id')      
            ) for workflow_response in workflows_response
        ]
        return workflows
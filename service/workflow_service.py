from model import Workflow
from repository import WorkflowRepository
from controller import common_controller as common_ctrl
from utils import Singleton


log = common_ctrl.log


class WorkflowService(metaclass=Singleton):


    def __init__(self, workflow_repository: WorkflowRepository) -> None:
        self.workflow_repository = workflow_repository


    def save_workflow(self, workflow: Workflow) -> 'Workflow':
        """
        Saves a workflow using the workflow repository.

        Args:
            workflow (Workflow): The workflow to be saved.

        Returns:
            Workflow: The created workflow object.
        """
        log.info('Calling repository to save workflow. workflowId: %s, organizationId: %s', workflow.workflow_id, workflow.owner_id)
        created_workflow = self.workflow_repository.save(workflow)
        return created_workflow
    

    def get_data_studio_workflows(self, owner_id:str) -> list[Workflow]:
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

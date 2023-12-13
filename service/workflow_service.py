from model import Workflow
from repository import WorkflowRepository
from controller import common_controller as common_ctrl


log = common_ctrl.log


class WorkflowService:


    _instance = None


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


    @classmethod
    def get_instance(cls, workflow_repository: WorkflowRepository, prefer=None):
        """
        Creates and returns an instance of the WorkflowService class.

        Parameters:
            workflow_repository (WorkflowRepository): The WorkflowRepository object used to save and retrieve workflows.
            prefer (Optional): An optional WorkflowRepository object that will be used as the instance if provided.

        Returns:
            WorkflowRepository: The instance of the WorkflowRepository class.
        """
        if not cls._instance:
            cls._instance = prefer if prefer else WorkflowService(workflow_repository)

        return cls._instance

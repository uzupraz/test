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

from model import Workflow
from repository import WorkflowRepository
from controller import common_controller as common_ctrl


log = common_ctrl.log


class WorkflowService:


    def __init__(self, workflow_repository: WorkflowRepository) -> None:
        self.workflow_repository = workflow_repository


    def save_workflow(self, workflow: Workflow):
        self.workflow_repository.save(workflow)

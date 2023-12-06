from model import Workflow
from .workflow_converter import StepFunctionJSONConverter


class WorkflowService:


    def __init__(self, workflow_converter: StepFunctionJSONConverter) -> None:
        self.workflow_converter = workflow_converter


    def convert_to_step_function(self, workflow: Workflow):
        self.workflow_converter.convert(workflow)
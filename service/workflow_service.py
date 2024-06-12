from model import Workflow, WorkflowStats, WorkflowExecutionEvent, WorkflowFailedEvent, WorkflowFailure, WorkflowIntegration
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


    def get_workflow_integrations(self, owner_id: str, start_date:str, end_date:str) -> list[WorkflowIntegration]:
        """
        Get all the active workflow integrations using workflow repository.

        Args:
            owner_id (str): ID associated with user to filter workflow integrations by.
            start_date (str): Start date for the workflow integrations.
            end_date (str): End date for the workflow integrations.

        Returns:
            workflow_integrations (list[WorkflowIntegration]): Active workflow integrations.
        """
        log.info('Calling repository to get workflow integrations. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        workflow_integrations = self.workflow_repository.get_workflow_integrations(owner_id, start_date, end_date)
        return workflow_integrations
    

    def get_workflow_failed_events(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowFailedEvent]:
        """
        Get workflow failed events using the workflow repository.

        Args:
            owner_id (str): ID associated with user to filter workflow failed events by.
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            workflow_failed_events (list[WorkflowFailedEvent]): Workflow failed events.
        """
        log.info('Calling repository to get workflow failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        workflow_failed_events = self.workflow_repository.get_workflow_failed_events(owner_id, start_date, end_date)
        return workflow_failed_events
    

    def get_workflow_failures(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowFailure]:
        """
        Get workflow failures using the workflow repository.

        Args:
            owner_id (str): ID associated with user to filter workflow failures by.
            start_date (str): Start date for the failures.
            end_date (str): End date for the failures.

        Returns:
            workflow_failures (list[WorkflowFailure]): Workflow failures.
        """
        log.info('Calling repository to get workflow failures. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        workflow_failures = self.workflow_repository.get_workflow_failures(owner_id, start_date, end_date)
        return workflow_failures
    
    
    @classmethod
    def get_instance(cls, workflow_repository: WorkflowRepository, prefer=None) -> 'WorkflowService':
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

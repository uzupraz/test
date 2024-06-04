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
    
    
    def get_workflow_stats(self, start_date:str, end_date:str) -> dict[str, int|str]:
        """
        Get the stats about the workflows using the workflow repository.

        Args:
            start_date: Start date for the stats.
            end_date: End date for the stats.

        Returns:
            active_workflows: Number of active workflows.
            failed_events: Number of failed events.
            fluent_executions: Number of fluent executions.
            system_status: System status.
        """
        log.info('Calling repository to get workflow stats. start_date: %s, end_date: %s',start_date, end_date)
        workflow_stats = self.workflow_repository.get_workflow_stats(start_date, end_date)
        return workflow_stats
    

    def get_workflow_integrations(self, start_date:str, end_date:str) -> list[dict[str, any]]:
        """
        Get all the active workflow integrations using workflow repository.

        Args:
            start_date (str): Start date for the workflow integrations.
            end_date (str): End date for the workflow integrations.

        Returns:
            dict[str, any]: Active workflow integrations.
        """
        log.info('Calling repository to get workflow integrations. start_date: %s, end_date: %s',start_date, end_date)
        workflow_integrations = self.workflow_repository.get_workflow_integrations(start_date, end_date)
        return workflow_integrations
    

    def get_workflow_execution_events(self,start_date: str, end_date: str) -> list[dict[str, any]]:
        """
        Get workflow execution events using the workflow repository.

        Args:
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            list[dict[str, any]]: Workflow execution events.
        """
        log.info('Calling repository to get workflow execution events. start_date: %s, end_date: %s',start_date, end_date)
        workflow_execution_events = self.workflow_repository.get_workflow_execution_events(start_date, end_date)
        return workflow_execution_events
    

    def get_workflow_failed_events(self, start_date: str, end_date: str) -> list[dict[str, any]]:
        """
        Get workflow failed events using the workflow repository.

        Args:
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            list[dict[str, any]]: Workflow failed events.
        """
        log.info('Calling repository to get workflow failed events. start_date: %s, end_date: %s',start_date, end_date)
        workflow_failed_events = self.workflow_repository.get_workflow_failed_events(start_date, end_date)
        return workflow_failed_events
    

    def get_workflow_failures(self, start_date: str, end_date: str) -> list[dict[str, any]]:
        """
        Get workflow failures using the workflow repository.

        Args:
            start_date (str): Start date for the failures.
            end_date (str): End date for the failures.

        Returns:
            list[dict[str, any]]: Workflow failures.
        """
        log.info('Calling repository to get workflow failures. start_date: %s, end_date: %s',start_date, end_date)
        workflow_failures = self.workflow_repository.get_workflow_failures(start_date, end_date)
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

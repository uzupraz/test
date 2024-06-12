from controller import common_controller as common_ctrl
from repository import WorkflowRepository
from service import OpensearchService
from model import WorkflowStats, WorkflowExecutionEvent


log = common_ctrl.log


class DashboardService:


    _instance = None


    def __init__(self, workflow_repository:WorkflowRepository, opensearch_service:OpensearchService) -> None:
        self.workflow_repository = workflow_repository
        self.opensearch_service = opensearch_service


    def get_workflow_stats(self, owner_id: str, start_date:str, end_date:str) -> WorkflowStats:
        """
        Get the stats about the workflows from DynamoDB and OpenSearch.
        
        Parameters:
            owner_id(str): Owner ID for the workflow stats.
            start_date(str): Start date for the stats.
            end_date(str): End date for the stats.
        
        Returns:
            workflow_stats(WorkflowStats): The workflow stats.
        """
        log.info('Getting workflow stats. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        active_workflows = self.workflow_repository.count_active_workflows()
        failed_events = self.opensearch_service.get_failed_events_count(owner_id=owner_id, start_date=start_date, end_date=end_date)
        fluent_executions=self.opensearch_service.get_fluent_executions_count(owner_id=owner_id, start_date=start_date, end_date=end_date)

        return WorkflowStats(
            active_workflows=active_workflows,
            failed_events=failed_events,
            fluent_executions=fluent_executions,
            system_status="Online",
        )


    def get_workflow_execution_events(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowExecutionEvent]:
        """
        Get workflow execution events from OpenSearch.

        Args:
            owner_id (str): Owner ID for the events.
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            workflow_execution_events(list[WorkflowExecutionEvents]): Workflow execution events.
        """
        log.info('Getting workflow execution events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        return self.opensearch_service.get_execution_and_error_counts(owner_id, start_date, end_date)

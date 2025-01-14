from typing import List
from datetime import datetime
from controller import common_controller as common_ctrl
from repository import WorkflowRepository
from model import WorkflowStats, WorkflowExecutionMetric, WorkflowFailedEvent, WorkflowFailure, WorkflowError, WorkflowItem, WorkflowIntegration
from utils import Singleton
from repository import ExecutionSummaryRepository


log = common_ctrl.log


class DashboardService(metaclass=Singleton):


    def __init__(self, workflow_repository:WorkflowRepository, execution_summary_repository:ExecutionSummaryRepository) -> None:
        self.workflow_repository = workflow_repository
        self.execution_summary_repository = execution_summary_repository


    def get_workflow_stats(self, owner_id: str, start_date: datetime, end_date: datetime) -> WorkflowStats:
        """
        Get the stats about the workflows from DynamoDB and Postgres.

        Parameters:
            owner_id(str): Owner ID for the workflow stats.
            start_date(datetime): Start date for the stats.
            end_date(datetime): End date for the stats.

        Returns:
            workflow_stats(WorkflowStats): The workflow stats.
        """
        log.info('Getting workflow stats. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        execution_stats = self.execution_summary_repository.get_execution_stats(owner_id, start_timestamp, end_timestamp)
        execution_stats.active_workflows_count = self.workflow_repository.count_active_workflows(owner_id=owner_id)
        return execution_stats
        

    def get_workflow_execution_metrics_by_date(self, owner_id: str, start_date: datetime, end_date: datetime) -> List[WorkflowExecutionMetric]:
        """
        Get the metrics about the workflow executions from postgres.

        Parameters:
            owner_id(str): Owner ID for the workflow stats.
            start_date(datetime): Start date for the stats.
            end_date(datetime): End date for the stats.

        Returns:
            workflow_execution_metric(WorkflowExecutionMetric): The workflow execution metric.
        """
        log.info('Getting failed and total executions by date. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        return self.execution_summary_repository.get_workflow_execution_metrics_by_date(owner_id, start_timestamp, end_timestamp)
    

    def get_workflow_integrations(self, owner_id: str, start_date: datetime, end_date: datetime) -> List[WorkflowIntegration]:
        """
        Get all workflow integrations from postgres.

        Parameters:
            owner_id (str): Owner ID for the workflow integrations.
            start_date (str): Start date for the workflow integrations.
            end_date (str): End date for the workflow integrations.

        Returns:
            workflow_integrations(list[WorkflowIntegration]): Active workflow integrations.

        Raises:
            ServiceException: If there is an error while getting the workflow integrations.
        """
        log.info('Getting workflow integrations. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        return self.execution_summary_repository.get_workflow_integrations(owner_id, start_timestamp, end_timestamp)


    def get_workflow_failed_executions(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowFailedEvent]:
        """
        Get workflow failed executions from postgres.

        Args:
            owner_id (str): Owner ID for the events.
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            workflow_failed_events(list[WorkflowFailedEvent]): Workflow failed events.

        Raises:
            ServiceException: If there is an error while getting the workflow failed events.
        """
        log.info('Getting workflow failed executions. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        return self.execution_summary_repository.get_workflow_failed_executions(owner_id, start_timestamp, end_timestamp)


    def get_workflow_failures(self, owner_id: str, start_date:str, end_date:str) -> List[WorkflowFailure]:
        """
        Get workflow failures from postgres.

        Args:
            owner_id (str): The owner ID to filter workflows failures by.
            start_date (str): Start date for the query.
            end_date (str): End date for the query.

        Returns:
            workflow_failures(list[WorkflowFailure]): List of Workflow failures containing error codes and their occurences and severity.

        Raises:
            ServiceException: If there is an error while getting the workflow failures.
        """
        log.info('Getting workflow failures. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        failures = self.execution_summary_repository.get_workflow_failures(owner_id, start_timestamp, end_timestamp)
        workflow_failures: dict[str, WorkflowFailure] = {}
        for failure in failures:
            if not workflow_failures.get(failure.workflow_id, None):
                workflow_failures[failure.workflow_id] = WorkflowFailure(
                    workflow=WorkflowItem(id=failure.workflow_id, name=failure.workflow_name),
                    errors=[]
                )
            error = WorkflowError(occurrence=failure.error_occurrence, error_code=failure.error_code)
            workflow_failures[failure.workflow_id].errors.append(error)

        return [item for item in workflow_failures.values()]
        
            
    def _get_timestamps_from_iso_dates(self, start_date: datetime, end_date: datetime):
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        return start_timestamp, end_timestamp
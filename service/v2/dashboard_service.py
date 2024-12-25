from botocore.exceptions import ClientError
from typing import List, Dict, Any
from datetime import datetime
from controller import common_controller as common_ctrl
from repository import WorkflowRepository
from service.opensearch_service import OpensearchService
from model import WorkflowStats, WorkflowExecutionMetric, WorkflowFailedEvent, WorkflowFailure, WorkflowError, WorkflowIntegration, WorkflowItem
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus, SystemStatus, WorkflowErrorCode, WorkflowErrorSeverity
from repository import ExecutionSummaryRepository


log = common_ctrl.log


class DashboardService(metaclass=Singleton):


    def __init__(self, workflow_repository:WorkflowRepository, execution_summary_repository:ExecutionSummaryRepository) -> None:
        self.workflow_repository = workflow_repository
        self.execution_summary_repository = execution_summary_repository


    def get_workflow_stats(self, owner_id: str, start_date: str, end_date: str) -> WorkflowStats:
        """
        Get the stats about the workflows from DynamoDB and Postgres.

        Parameters:
            owner_id(str): Owner ID for the workflow stats.
            start_date(str): Start date for the stats.
            end_date(str): End date for the stats.

        Returns:
            workflow_stats(WorkflowStats): The workflow stats.
        """
        log.info('Getting workflow stats. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        total_executions_count, failed_executions_count = self.execution_summary_repository.get_execution_stats(owner_id, start_timestamp, end_timestamp)
        active_workflows_count = self.workflow_repository.count_active_workflows(owner_id=owner_id)
        return WorkflowStats(
            active_workflows_count=active_workflows_count, 
            failed_executions_count=failed_executions_count,
            total_executions_count=total_executions_count
        )
    

    def get_workflow_execution_metrics_by_date(self, owner_id: str, start_date: str, end_date: str) -> List[WorkflowExecutionMetric]:
        """
        Get the metrics about the workflow executions from DynamoDB and Postgres.

        Parameters:
            owner_id(str): Owner ID for the workflow stats.
            start_date(str): Start date for the stats.
            end_date(str): End date for the stats.

        Returns:
            workflow_execution_metric(WorkflowExecutionMetric): The workflow execution metric.
        """
        log.info('Getting workflow stats. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps_from_iso_dates(start_date, end_date)
        return self.execution_summary_repository.get_workflow_execution_metrics_by_date(owner_id, start_timestamp, end_timestamp)


    def _get_timestamps_from_iso_dates(self, start_date: str, end_date: str):
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)

        start_timestamp = int(start.timestamp())
        end_timestamp = int(end.timestamp())
        return start_timestamp, end_timestamp
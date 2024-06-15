from botocore.exceptions import ClientError

from controller import common_controller as common_ctrl
from repository import WorkflowRepository
from service.opensearch_service import OpensearchService
from model import WorkflowStats, WorkflowItem, WorkflowExecutionEvent, WorkflowFailedEvent, WorkflowFailure, WorkflowFailureItem, WorkflowIntegration
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus, SystemStatus


log = common_ctrl.log


class DashboardService(metaclass=Singleton):


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
        active_worflows_count = self.workflow_repository.count_active_workflows(owner_id=owner_id)
        failed_events_count = self.opensearch_service.get_failed_events_count(owner_id=owner_id, start_date=start_date, end_date=end_date)
        fluent_executions_count = self.opensearch_service.get_fluent_executions_count(owner_id=owner_id, start_date=start_date, end_date=end_date)

        return WorkflowStats(
            active_worflows_count=active_worflows_count,
            failed_events_count=failed_events_count,
            fluent_executions_count=fluent_executions_count,
            system_status=SystemStatus.ONLINE.value,
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


    def get_workflow_integrations(self, owner_id: str, start_date:str, end_date:str) -> list[WorkflowIntegration]:
        """
        Get all the active workflow integrations from OpenSearch.

        Parameters:
            owner_id (str): Owner ID for the workflow integrations.
            start_date (str): Start date for the workflow integrations.
            end_date (str): End date for the workflow integrations.

        Returns:
            workflow_integrations(list[WorkflowIntegration]): Active workflow integrations.

        Raises:
            ServiceException: If there is an error while getting the workflow integrations.
        """
        try:
            log.info('Getting workflow integrations. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            #! REPLACE THIS WITH REAL DB QUERY
            workflow_integrations = [
                WorkflowIntegration(
                    failure_count=2,
                    failure_ratio=0.2,
                    last_event_date="2021-07-01",
                    workflow=WorkflowItem(id="1", name="Workflow 1"),
                )
            ]
            return workflow_integrations
        except ClientError as e:
            log.exception('Failed to get workflow integrations.')
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Couldn\'t get workflow integrations')


    def get_workflow_failed_events(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowFailedEvent]:
        """
        Get workflow failed events from OpenSearch.

        Args:
            owner_id (str): Owner ID for the events.
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            workflow_failed_events(list[WorkflowFailedEvent]): Workflow failed events.

        Raises:
            ServiceException: If there is an error while getting the workflow failed events.
        """
        try:
            log.info('Getting workflow failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            #! REPLACE THIS WITH REAL DB QUERY
            workflow_failed_events = [
                WorkflowFailedEvent(
                    date="2021-07-01",
                    error_code="ERR-001",
                    event_id="1",
                    workflow=WorkflowItem(
                        id="1",
                        name="Workflow 1",
                    ),
                )
            ]
            return workflow_failed_events
        except ClientError as e:
            log.exception('Failed to get workflow failed events.')
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Couldn\'t get workflow failed events')


    def get_workflow_failures(self, owner_id: str, start_date:str, end_date:str) -> list[WorkflowFailure]:
        """
        Get workflow failures from OpenSearch.

        Args:
            owner_id (str): Owner ID for the failures.
            start_date (str): Start date for the failures.
            end_date (str): End date for the failures.

        Returns:
            workflow_failures(list[WorkflowFailure]): Workflow failures.

        Raises:
            ServiceException: If there is an error while getting the workflow failures.
        """
        try:
            log.info('Getting workflow failures. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            #! REPLACE THIS WITH REAL DB QUERY
            workflow_failures = [
                WorkflowFailure(
                    color="red",
                    workflow_name="Workflow 1",
                    failures=[
                        WorkflowFailureItem(
                            error_code="ERR-001",
                            failure_ratio=0.2,
                            severity=0.5,
                        )
                    ],
                )
            ]
            return workflow_failures
        except ClientError as e:
            log.exception('Failed to get workflow failures.')
            raise ServiceException(e.response['ResponseMetadata']['HTTPStatusCode'], ServiceStatus.FAILURE, 'Coulnd\'t get workflow failures')

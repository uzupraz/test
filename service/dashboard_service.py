from botocore.exceptions import ClientError

from controller import common_controller as common_ctrl
from repository import WorkflowRepository
from service.opensearch_service import OpensearchService
from model import WorkflowStats, WorkflowExecutionMetric, WorkflowFailedEvent, WorkflowFailure, WorkflowFailureItem, WorkflowIntegration, WorkflowItem
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus, SystemStatus


log = common_ctrl.log


class DashboardService(metaclass=Singleton):


    def __init__(self, workflow_repository:WorkflowRepository, opensearch_service:OpensearchService) -> None:
        self.workflow_repository = workflow_repository
        self.opensearch_service = opensearch_service


    def get_workflow_stats(self, owner_id: str, start_date: str, end_date: str) -> WorkflowStats:
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
        active_workflows_count = self.workflow_repository.count_active_workflows(owner_id=owner_id)
        response = self.opensearch_service.get_executions_metrics(owner_id, start_date, end_date)

        return self._map_workflow_stats(response, active_workflows_count)


    def get_workflow_execution_metrics_by_date(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowExecutionMetric]:
        """
        Get workflow execution events from OpenSearch by date.

        Args:
            owner_id (str): Owner ID for the events.
            start_date (str): Start date for the events.
            end_date (str): End date for the events.

        Returns:
            workflow_execution_events(list[WorkflowExecutionMetric]): Workflow execution events.
        """
        log.info('Getting workflow execution events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        response = self.opensearch_service.get_execution_metrics_by_date(owner_id, start_date, end_date)
        metrics = [
            self._map_workflow_execution_metrics_by_date(bucket)
            for bucket in response["aggregations"]["by_date"]["buckets"]
        ]
        return metrics


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
        log.info('Getting workflow integrations. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        response =  self.opensearch_service.get_workflow_integrations(owner_id, start_date, end_date)
        workflow_integrations = [
            self._map_workflow_integration_bucket(bucket)
            for bucket in response["aggregations"]["integrations"]["buckets"]
        ]
        return workflow_integrations


    def get_workflow_failed_executions(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowFailedEvent]:
        """
        Get workflow failed executions from OpenSearch.

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
        response = self.opensearch_service.get_workflow_failed_executions(owner_id, start_date, end_date)
        workflow_failed_executions = self._map_workflow_failed_executions_response(response)
        return workflow_failed_executions


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


    def _map_workflow_stats(self, response:dict, active_workflows_count:int) -> WorkflowStats:
        """
        Maps the response returned by querying for workflow_stats to  WorkflowStats object.
        """
        # Extract metrics from the response
        total_executions = response['aggregations']['total_executions']['value']
        failed_executions = response['aggregations']['failed_executions']['failed_count']['value']
        return WorkflowStats(
            active_workflows_count=active_workflows_count,
            failed_executions_count=failed_executions,
            total_executions_count=total_executions,
            system_status=SystemStatus.ONLINE.value
        )


    def _map_workflow_execution_metrics_by_date(self, bucket:dict) -> WorkflowExecutionMetric:
        """
        Maps the bucket returned by querying for workflow_execution_metrics_by_date to a WorkflowExecutionMetric object.
        """
        return WorkflowExecutionMetric(
                date=bucket["key_as_string"],
                failed_executions=bucket["failed_executions"]["failed_count"]["value"],
                total_executions=bucket["total_executions"]["value"],
                )


    def _map_workflow_integration_bucket(self, bucket: dict) -> WorkflowIntegration:
        """
        Maps the bucket returned by querying for workflow_integrations to a WorkflowIntegration object.
        """
        workflow_name = bucket["workflow_name"]["buckets"][0]["key"]
        workflow_id = bucket["key"]
        last_event_date = bucket["last_event_date"]["value"]
        failed_executions_count = bucket["failed_executions"]["unique_executions"][
            "value"
        ]
        total_executions_count = bucket["total_executions"]["value"]
        failed_executions_ratio = (
            ((failed_executions_count / total_executions_count) * 100)
            if total_executions_count > 0
            else 0
        )

        return WorkflowIntegration(
            workflow=WorkflowItem(
                id=workflow_id,
                name=workflow_name,
            ),
            last_event_date=last_event_date,
            failed_executions_count=failed_executions_count,
            failed_executions_ratio=failed_executions_ratio,
        )
    

    def _map_workflow_failed_executions_response(self, response: dict) -> list[WorkflowFailedEvent]:
        """
        Maps the response returned by querying for workflow_failed_executions to a list of WorkflowFailedEvent objects.
        """
        buckets = response["aggregations"]["by_date"]["buckets"]
        workflow_failed_events: list[WorkflowFailedEvent] = []

        for bucket in buckets:
            date = bucket["key_as_string"]
            nested_buckets = bucket["failed_executions"]["buckets"]
            for nested_bucket in nested_buckets:
                event_id = nested_bucket["event_id"]["buckets"][0]["key"]
                workflow_name = nested_bucket["workflow_name"]["buckets"][0]["key"]
                workflow_id = nested_bucket["workflow_id"]["buckets"][0]["key"]
                error_code = nested_bucket["error_code"]["buckets"][0]["key"] if nested_bucket["error_code"]["buckets"] else None

                workflow_failed_event = WorkflowFailedEvent(
                    date=date,
                    workflow=WorkflowItem(
                        id=workflow_id,
                        name=workflow_name,
                    ),
                    error_code=error_code,
                    event_id=event_id,
                )
                workflow_failed_events.append(workflow_failed_event)

        return workflow_failed_events

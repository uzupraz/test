import unittest
from unittest.mock import MagicMock, patch


from model import WorkflowExecutionMetric, WorkflowFailedEvent, WorkflowIntegration, WorkflowItem, WorkflowStats
from repository import WorkflowRepository
from service import DashboardService, OpensearchService
from enums import SystemStatus
from tests.test_utils import TestUtils


class TestDashboardService(unittest.TestCase):
    

    test_resource_path = '/tests/resources/dashboard/'
    test_opensearch_resource_path = '/tests/resources/opensearch/'


    def setUp(self) -> None:
        app_config = MagicMock()
        aws_config = MagicMock()
        
        self.opensearch_config = MagicMock()
        self.opensearch_config.host = "test_host"
        self.opensearch_config.port = 443
        self.opensearch_config.pool_maxsize = 10
        self.opensearch_config.index = "test_index"
        self.opensearch_config.region = "us-east-1"
        self.opensearch_config.service = "es"

        self.workflow_repository = WorkflowRepository(app_config, aws_config)
        self.opensearch_service = OpensearchService(self.opensearch_config)
        self.dashboard_service = DashboardService(self.workflow_repository, self.opensearch_service)
    

    @patch("service.dashboard_service.WorkflowRepository.count_active_workflows")
    @patch("service.dashboard_service.OpensearchService.get_executions_metrics")
    def test_get_workflow_stats(self, mock_get_executions_metrics, mock_count_active_workflows):
        """
        Tests whether this function correctly returns the workflow stats.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"
        mock_response_path = self.test_opensearch_resource_path + "get_executions_metrics_response.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_get_executions_metrics.return_value = mock_response
        mock_count_active_workflows.return_value = 10

        actual_result = self.dashboard_service.get_workflow_stats(owner_id, start_date, end_date)
        expected_result = WorkflowStats(
            active_workflows_count=10,
            failed_executions_count=6,
            total_executions_count=126,
            system_status=SystemStatus.ONLINE.value
        )

        self.assertEqual(actual_result, expected_result)
        mock_get_executions_metrics.assert_called_with(owner_id, start_date, end_date)
        mock_count_active_workflows.assert_called_with(owner_id=owner_id)


    @patch("service.dashboard_service.OpensearchService.get_execution_metrics_by_date")
    def test_get_workflow_execution_metrics_by_date(self, mock_get_execution_metrics_by_date):
        """
        Tests whether this function correctly returns the workflow execution metrics by date.
        """
        owner_id = "owner_id"
        start_date = "2024-06-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.test_resource_path + "get_workflow_execution_metrics_by_date_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_execution_metrics_by_date.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_execution_metrics_by_date(owner_id, start_date, end_date)
        expected_result = [
            WorkflowExecutionMetric(
                date="2024-06-23",
                failed_executions=0,
                total_executions=2,
            ),
            WorkflowExecutionMetric(
                date="2024-06-24",
                failed_executions=0,
                total_executions=2,
            ),
            WorkflowExecutionMetric(
                date="2024-06-25",
                failed_executions=0,
                total_executions=2,
            )
        ]

        self.assertEqual(actual_result, expected_result)
        mock_get_execution_metrics_by_date.assert_called_with(owner_id, start_date, end_date)
    

    @patch("service.dashboard_service.OpensearchService.get_workflow_integrations")
    def test_get_workflow_integrations(self, mock_get_workflow_integrations):
        """
        Tests whether this function correctly returns the workflow integrations.
        """
        owner_id = "owner_id"
        start_date = "2024-06-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.test_resource_path + "get_workflow_integrations_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_integrations.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_integrations(owner_id, start_date, end_date)
        expected_result = [
            WorkflowIntegration(
                failed_executions_count=0,
                failed_executions_ratio=0,
                last_event_date=1719313208000,
                workflow=WorkflowItem(
                    id="KZlnumlwuVqnMoNGC9Rrj",
                    name="Workflow to convert JSON into WA ITC.",
                )
            )
        ]

        self.assertEqual(actual_result, expected_result)
        mock_get_workflow_integrations.assert_called_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_failed_executions")
    def test_get_workflow_failed_executions(self, mock_get_workflow_failed_executions):
        """
        Tests whether this function correctly returns the workflow failed executions.
        """
        owner_id = "owner_id"
        start_date = "2024-04-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.test_resource_path + "get_workflow_failed_executions_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_failed_executions.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_failed_executions(owner_id, start_date, end_date)
        expected_result = [
            WorkflowFailedEvent(
                date="2024-05-27",
                workflow=WorkflowItem(
                    id="VeDYTvy56weuVExSaPIqO",
                    name="Workflow to convert JSON into WE ITC.",
                ),
                error_code=None,
                event_id="Cg4xnePTpLeqXTDONo0Ke"
            ),
            WorkflowFailedEvent(
                date="2024-05-27",
                workflow=WorkflowItem(
                    id="VeDYTvy56weuVExSaPIqO",
                    name="Workflow to convert JSON into WE ITC.",
                ),
                error_code=None,
                event_id="AIhZRwq0AR9O3VVJmWAjj"
            )
        ]

        self.assertEqual(actual_result, expected_result)
        mock_get_workflow_failed_executions.assert_called_with(owner_id, start_date, end_date)
    
import unittest
from unittest.mock import MagicMock, patch

from utils import Singleton
from model import WorkflowExecutionMetric, WorkflowFailedEvent, WorkflowIntegration, WorkflowItem, WorkflowStats, WorkflowFailure, WorkflowError
from repository import WorkflowRepository
from service import DashboardService, OpensearchService
from enums import SystemStatus, WorkflowErrorCode, WorkflowErrorSeverity
from tests.test_utils import TestUtils


class TestDashboardService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/dashboard/'


    def setUp(self) -> None:
        Singleton.clear_instance(WorkflowRepository)
        Singleton.clear_instance(OpensearchService)
        Singleton.clear_instance(DashboardService)

        app_config = MagicMock()
        aws_config = MagicMock()
        aws_config.is_local = True
        aws_config.dynamodb_aws_region = "local"

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


    def tearDown(self) -> None:
        del self.workflow_repository
        del self.opensearch_service
        del self.dashboard_service

        Singleton.clear_instance(WorkflowRepository)
        Singleton.clear_instance(OpensearchService)
        Singleton.clear_instance(DashboardService)


    @patch("service.dashboard_service.WorkflowRepository.count_active_workflows")
    @patch("service.dashboard_service.OpensearchService.get_executions_metrics")
    def test_get_workflow_stats(self, mock_get_executions_metrics, mock_count_active_workflows):
        """
        Tests whether this function correctly returns the workflow stats.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"
        mock_response_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_response.json"

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


    @patch("service.dashboard_service.WorkflowRepository.count_active_workflows")
    @patch("service.dashboard_service.OpensearchService.get_executions_metrics")
    def test_get_workflow_stats_for_invalid_field_in_response(self, mock_get_executions_metrics, mock_count_active_workflows):
        """
        Tests whether this function raises an Key error when invalid field is returned from open search response.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"
        mock_response_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_with_invalid_field_response.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_get_executions_metrics.return_value = mock_response
        mock_count_active_workflows.return_value = 10

        with self.assertRaises(KeyError) as context:
            self.dashboard_service.get_workflow_stats(owner_id, start_date, end_date)

        self.assertIn('failed_count', str(context.exception))
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

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_execution_metrics_by_date_response.json"
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


    @patch("service.dashboard_service.OpensearchService.get_execution_metrics_by_date")
    def test_get_workflow_execution_metrics_by_date_for_invalid_field_in_response(self, mock_get_execution_metrics_by_date):
        """
        Tests whether this function raises an Key error when invalid field is returned from open search response.
        """
        owner_id = "owner_id"
        start_date = "2024-06-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_execution_metrics_by_date_with_invalid_field_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_execution_metrics_by_date.return_value = mock_response

        with self.assertRaises(KeyError) as context:
            self.dashboard_service.get_workflow_execution_metrics_by_date(owner_id, start_date, end_date)

        self.assertIn('failed_count', str(context.exception))
        mock_get_execution_metrics_by_date.assert_called_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_integrations")
    def test_get_workflow_integrations(self, mock_get_workflow_integrations):
        """
        Tests whether this function correctly returns the workflow integrations.
        """
        owner_id = "owner_id"
        start_date = "2024-06-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_integrations.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_integrations(owner_id, start_date, end_date)
        expected_result = [
            WorkflowIntegration(
                failed_executions_count=0,
                total_executions_count=66,
                failed_executions_ratio=0,
                last_event_date="1719313208",
                workflow=WorkflowItem(
                    id="KZlnumlwuVqnMoNGC9Rrj",
                    name="Workflow to convert JSON into WA ITC.",
                )
            )
        ]

        self.assertEqual(actual_result, expected_result)
        mock_get_workflow_integrations.assert_called_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_integrations")
    def test_get_workflow_integrations_for_invalid_field_in_response(self, mock_get_workflow_integrations):
        """
        Tests whether this function raises an Key error when invalid field is returned from open search response.
        """
        owner_id = "owner_id"
        start_date = "2024-06-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_with_invalid_field_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_integrations.return_value = mock_response

        with self.assertRaises(KeyError) as context:
            self.dashboard_service.get_workflow_integrations(owner_id, start_date, end_date)

        self.assertIn('failed_executions', str(context.exception))
        mock_get_workflow_integrations.assert_called_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_failed_executions")
    def test_get_workflow_failed_executions(self, mock_get_workflow_failed_executions):
        """
        Tests whether this function correctly returns the workflow failed executions.
        """
        owner_id = "owner_id"
        start_date = "2024-04-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_failed_executions.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_failed_executions(owner_id, start_date, end_date)
        expected_result = [
            WorkflowFailedEvent(
                date="2024-05-27",
                error_code=None,
                event_id="Cg4xnePTpLeqXTDONo0Ke",
                execution_id="1WivE8vEsxggA_JQt0TyR",
                workflow=WorkflowItem(
                    id="VeDYTvy56weuVExSaPIqO",
                    name="Workflow to convert JSON into WE ITC.",
                )
            ),
            WorkflowFailedEvent(
                date="2024-05-27",
                error_code=None,
                event_id="AIhZRwq0AR9O3VVJmWAjj",
                execution_id="9reWJ1QH8_6_wmtIStH8N",
                workflow=WorkflowItem(
                    id="VeDYTvy56weuVExSaPIqO",
                    name="Workflow to convert JSON into WE ITC.",
                )
            )
        ]

        self.assertEqual(actual_result, expected_result)
        mock_get_workflow_failed_executions.assert_called_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_failed_executions")
    def test_get_workflow_failed_executions_for_invalid_field_in_response(self, mock_get_workflow_failed_executions):
        """
        Tests whether this function raises an Key error when invalid field is returned from open search response.
        """
        owner_id = "owner_id"
        start_date = "2024-04-22T11:28:38.317142"
        end_date = "2024-06-26T11:28:38.317142"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_with_invalid_field_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_failed_executions.return_value = mock_response

        with self.assertRaises(KeyError) as context:
            self.dashboard_service.get_workflow_failed_executions(owner_id, start_date, end_date)

        self.assertIn('workflow_name', str(context.exception))
        mock_get_workflow_failed_executions.assert_called_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_failures")
    def test_get_workflow_failures_happy_case(self, mock_get_workflow_failures):
        """
        Tests whether this function correctly returns the workflow failures and transforms the opensearch response to the desired output.
        """
        owner_id = "owner_id"
        start_date = "2024-09-20T00:00:00.908Z"
        end_date = "2024-09-26T11:59:24.908Z"

        mock_response_path = '/tests/resources/opensearch/get_workflow_failures_query_response.json'
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_failures.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_failures(owner_id, start_date, end_date)
        expected_result = [
            WorkflowFailure(
                workflow=WorkflowItem(
                    id='workflow_1',
                    name='Test Workflow 1'
                ),
                errors=[
                    WorkflowError(
                        occurrence=9,
                        error_code=WorkflowErrorCode.UNKNOWN.value,
                        severity=WorkflowErrorSeverity.HIGH.value
                    )
                ]
            ),
            WorkflowFailure(
                workflow=WorkflowItem(
                    id='workflow_2',
                    name='Test Workflow 2'
                ),
                errors=[
                    WorkflowError(
                        occurrence=1,
                        error_code=WorkflowErrorCode.UNKNOWN.value,
                        severity=WorkflowErrorSeverity.HIGH.value
                    )
                ]
            )
        ]

        self.assertEqual(actual_result, expected_result)
        mock_get_workflow_failures.assert_called_once_with(owner_id, start_date, end_date)


    @patch("service.dashboard_service.OpensearchService.get_workflow_failures")
    def test_get_workflow_failures_should_return_empty_list(self, mock_get_workflow_failures):
        """
        Tests whether this function correctly returns an empty list when the opensearch query returns no results.
        """
        owner_id = "owner_id"
        start_date = "2024-09-20T00:00:00.908Z"
        end_date = "2024-09-26T11:59:24.908Z"

        mock_response_path = '/tests/resources/opensearch/get_workflow_failures_query_empty_data_response.json'
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_get_workflow_failures.return_value = mock_response

        actual_result = self.dashboard_service.get_workflow_failures(owner_id, start_date, end_date)

        self.assertListEqual(actual_result, [])
        mock_get_workflow_failures.assert_called_once_with(owner_id, start_date, end_date)

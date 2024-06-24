import unittest
from unittest.mock import MagicMock, patch
from enums.status import ServiceStatus
from exception.service_exception import ServiceException
from model import WorkflowExecutionMetric, WorkflowIntegration, WorkflowItem, WorkflowFailedEvent
from service.opensearch_service import OpensearchService
from tests.test_utils import TestUtils

class TestOpensearchService(unittest.TestCase):


    test_resource_path = '/tests/resources/opensearch_service/'


    def setUp(self):
        self.config = MagicMock()
        self.config.host = "test_host"
        self.config.port = 443
        self.config.pool_maxsize = 10
        self.config.index = "test_index"
        self.config.region = "us-east-1"
        self.config.service = "es"
        self.service = OpensearchService(self.config)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics(self, mock_search):
        """
        Tests whether this function correctly returns the total_executions and failed_executions count.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_executions_metrics_response.json"
        mock_query_path = self.test_resource_path + "get_executions_metrics_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)
        actual_result = {
            "total_executions": 126,
            "failed_executions": 6
        }

        self.assertEqual(response, actual_result)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date(self, mock_search):
        """
        Tests whether this function correctly returns count of failed_executions and total_executions by date.
        """
        owner_id = "owner_id"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_execution_metrics_by_date_response.json"
        mock_query_path = self.test_resource_path + "get_execution_metrics_by_date_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)
        actual_result = [
            WorkflowExecutionMetric(
                date="2024-06-17",
                total_executions=6,
                failed_executions=0,
            ),
            WorkflowExecutionMetric(
                date="2024-06-18",
                total_executions=9,
                failed_executions=0,
            ),
            WorkflowExecutionMetric(
                date="2024-06-19",
                total_executions=6,
                failed_executions=0,
            ),
            WorkflowExecutionMetric(
                date="2024-06-20",
                total_executions=2,
                failed_executions=0,
            ),
        ]

        self.assertEqual(response, actual_result)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations(self, mock_search):
        owner_id = "owner_id"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_integrations_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_integrations_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response

        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)
        actual_result = [
            WorkflowIntegration(
                workflow=WorkflowItem(
                    name="Workflow to convert JSON into WA ITC.",
                    id="KZlnumlwuVqnMoNGC9Rrj",
                ),
                failed_executions_count=0,
                failed_executions_ratio=0,
                last_event_date=1718794823000
            )
        ]

        self.assertEqual(response, actual_result)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions(self, mock_search):
        """
        Tests whether this function correctly returns the failed executions of a workflow by date.
        """
        owner_id = "owner_id"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_failed_executions_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_failed_executions_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response

        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)
        actual_result = [
            WorkflowFailedEvent(
                date="2024-05-27",
                error_code=None,
                workflow=WorkflowItem(
                    name="Workflow to convert JSON into WE ITC.",
                    id="VeDYTvy56weuVExSaPIqO"
                ),
                event_id="Cg4xnePTpLeqXTDONo0Ke"
            ),
            WorkflowFailedEvent(
                date="2024-05-27",
                error_code=None,
                workflow=WorkflowItem(
                    name="Workflow to convert JSON into WE ITC.",
                    id="VeDYTvy56weuVExSaPIqO"
                ),
                event_id="AIhZRwq0AR9O3VVJmWAjj"
            ),
        ]

        self.assertEqual(response, actual_result)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_execute_query_exeception(self, mock_search):
        mock_search.side_effect = ServiceException(500, ServiceStatus.FAILURE, "error")
        with self.assertRaises(ServiceException):
            self.service._execute_query(
                query={},
                owner_id="owner_id",
                start_date="2024-06-17T06:15:20.678Z",
                end_date="2024-06-19T06:15:20.678Z",
            )


    @patch("service.opensearch_service.OpenSearch.search")
    def test_execute_query_invalid_date_exception(self, mock_search):
        """
        Test if the function raises a ServiceException when the date is invalid.
        """
        mock_search.side_effect = ServiceException(400, ServiceStatus.FAILURE, "error")
        with self.assertRaises(ServiceException):
            self.service._execute_query(
                query={},
                owner_id="owner_id",
                start_date="invalid_start_date",
                end_date="invalid_end_date",
            )

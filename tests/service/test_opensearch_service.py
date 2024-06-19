import unittest
from unittest.mock import MagicMock, patch
from enums.status import ServiceStatus
from exception.service_exception import ServiceException
from model.dashboard import WorkflowExecutionMetric
from service.opensearch_service import OpensearchService
from tests.test_utils import TestUtils

class TestOpensearchService(unittest.TestCase):

    test_resource_path = '/tests/resources/opensearch_service/'
    histogram_aggregation = {
        "by_date": {
            "date_histogram": {
                "field": "event_timestamp",
                "calendar_interval": "day",
                "format": "yyyy-MM-dd",
            },
            "aggs": {
                "unique_executions": {"cardinality": {"field": "execution_id.keyword"}}
            },
        }
    }

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
    @patch("service.opensearch_service.OpensearchService._build_base_query")
    @patch("service.opensearch_service.OpensearchService._build_histogram_aggregation")
    def test_get_workflow_executions_count(self, mock_build_histogram_aggregation, mock_build_base_query, mock_search):
        """
        Test if the function returns the correct count of fluent executions of an workflow.
        """
        owner_id = "owner_id"
        start_date = "2024-05-22T06:15:20.678Z"
        end_date = "2024-06-19T06:15:20.678Z"

        mock_build_histogram_aggregation.return_value = self.histogram_aggregation
        mock_build_base_query.return_value = {
            "bool": {
                "filter": [
                    {"match_phrase": {"owner_id": owner_id}},
                    {"match_phrase": {"is_external": False}},
                    {
                        "range": {
                            "event_timestamp": {
                                "gte": start_date,
                                "lte": end_date,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ]
            }
        }

        query = {
            "size": 0,
            "query": self.service._build_base_query(owner_id, start_date=start_date, end_date=end_date),
            "aggs": self.service._build_histogram_aggregation()
        }

        mock_response_path = self.test_resource_path + "workflow_executions_count_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_search.return_value = mock_response
        mock_count = 230
        actual_count = self.service.get_workflow_executions_count(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(actual_count, mock_count)
        mock_search.assert_called_once_with(body=query, index=self.service.index)

    @patch("service.opensearch_service.OpenSearch.search")
    @patch("service.opensearch_service.OpensearchService._build_base_query")
    @patch("service.opensearch_service.OpensearchService._build_histogram_aggregation")
    def test_get_failed_events_executions_count(self, mock_build_histogram_aggregation, mock_build_base_query, mock_search):
        """
        Test if the function returns the correct count of failed events of an workflow.
        """
        owner_id = "owner_id"
        start_date = "2024-06-01T06:15:20.678Z"
        end_date = "2024-06-19T06:15:20.678Z"

        mock_build_histogram_aggregation.return_value = self.histogram_aggregation
        mock_build_base_query.return_value = {
            "bool": {
                "filter": [
                    {"match_phrase": {"owner_id": owner_id}},
                    {"match_phrase": {"is_external": False}},
                    {
                        "range": {
                            "event_timestamp": {
                                "gte": start_date,
                                "lte": end_date,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ]
            }
        }

        query = {
            "size": 0,
            "query": self.service._build_base_query(owner_id, start_date=start_date, end_date=end_date),
            "aggs": self.service._build_histogram_aggregation()
        }

        mock_response_path = self.test_resource_path + "failed_events_executions_count_response.json"
        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_search.return_value = mock_response
        mock_count = 159
        actual_count = self.service.get_failed_events_executions_count(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(actual_count, mock_count)
        mock_search.assert_called_once_with(body=query, index=self.service.index)

    @patch("service.opensearch_service.OpensearchService._fetch_fluent_executions")
    @patch("service.opensearch_service.OpensearchService._fetch_failed_events")
    def test_get_execution_and_error_counts(self, mock_fetch_failed_events, mock_fetch_fluent_executions):
        """
        Test if the function returns the correct count of fluent executions and failed events of an workflow on a certain date.
        """
        owner_id = "owner_id"
        start_date = "2024-06-17T06:15:20.678Z"
        end_date = "2024-06-19T06:15:20.678Z"

        mock_failed_events_response_path = self.test_resource_path + "failed_events_response.json"
        mock_failed_events_response = TestUtils.get_file_content(mock_failed_events_response_path)

        mock_fluent_executions_response_path = self.test_resource_path + "fluent_executions_response.json"
        mock_fluent_executions_response = TestUtils.get_file_content(mock_fluent_executions_response_path)

        mock_fetch_failed_events.return_value = mock_failed_events_response
        mock_fetch_fluent_executions.return_value = mock_fluent_executions_response

        results = self.service.get_execution_and_error_counts(owner_id=owner_id, start_date=start_date, end_date=end_date)

        expected_results = [
            WorkflowExecutionMetric(
                date="2024-06-17",
                failed_events=1,
                fluent_executions=3,
            ),
            WorkflowExecutionMetric(
                date="2024-06-18",
                failed_events=3,
                fluent_executions=9,
            ),
            WorkflowExecutionMetric(
                date="2024-06-19",
                failed_events=2,
                fluent_executions=4,
            ),
        ]

        self.assertEqual(results, expected_results)
        mock_fetch_failed_events.assert_called_once_with(owner_id=owner_id, start_date=start_date, end_date=end_date)
        mock_fetch_fluent_executions.assert_called_once_with(owner_id=owner_id, start_date=start_date, end_date=end_date)


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

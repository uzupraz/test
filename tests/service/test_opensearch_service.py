import unittest
from unittest.mock import patch
from model.dashboard import WorkflowExecutionEvent
from service.opensearch_service import OpensearchService
from configuration import OpensearchConfig

class TestOpensearchService(unittest.TestCase):


    def setUp(self):
        self.config = OpensearchConfig()
        self.config.host = "test_host"
        self.config.port = 443
        self.config.pool_maxsize = 10
        self.config.index = "test_index"
        self.config.region = "us-east-1"
        self.config.service = "es"
        self.service = OpensearchService(self.config)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_fluent_executions_count(self, mock_search):
        """
        Test if the function returns the correct count of fluent executions of an workflow.
        """
        mock_response = {
            "aggregations": {
                "by_date": {
                    "buckets": [
                        {
                            "key_as_string": "2021-01-01",
                            "unique_executions": {"value": 5},
                        },
                        {
                            "key_as_string": "2021-01-02",
                            "unique_executions": {"value": 3},
                        },
                    ]
                }
            }
        }
        mock_search.return_value = mock_response
        mock_count = 8
        actual_count = self.service.get_fluent_executions_count("owner_id", "2021-01-01", "2021-01-02")
        self.assertEqual(actual_count, mock_count)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_failed_events_count(self, mock_search):
        """
        Test if the function returns the correct count of failed events of an workflow.
        """
        mock_response = {
            "aggregations": {
                "by_date": {
                    "buckets": [
                        {
                            "key_as_string": "2021-01-01",
                            "unique_executions": {"value": 2},
                        },
                        {
                            "key_as_string": "2021-01-02",
                            "unique_executions": {"value": 1},
                        },
                    ]
                }
            }
        }
        mock_search.return_value = mock_response
        mock_count = 3
        actual_count = self.service.get_failed_events_count("owner_id", "2021-01-01", "2021-01-02")
        self.assertEqual(actual_count, mock_count)


    @patch("service.opensearch_service.OpensearchService.fetch_aggregated_data")
    def test_get_execution_and_error_counts(self, mock_fetch_aggregated_data):
        """
        Test if the function returns the correct count of fluent executions and failed events of an workflow on a certain date.
        """
        mock_fluent_executions_counts = {"2021-01-01": 5, "2021-01-02": 3}
        mock_failed_events_counts = {"2021-01-01": 2, "2021-01-02": 1}
        mock_fetch_aggregated_data.side_effect = [mock_fluent_executions_counts, mock_failed_events_counts]
        results = self.service.get_execution_and_error_counts("owner_id", "2021-01-01", "2021-01-02")
        expected_results = [
            WorkflowExecutionEvent(
                date="2021-01-01",
                failed_events=2,
                fluent_executions=5,
            ),
            WorkflowExecutionEvent(
                date="2021-01-02",
                failed_events=1,
                fluent_executions=3,
            ),
        ]
        self.assertEqual(results, expected_results)

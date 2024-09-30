import unittest
from unittest.mock import MagicMock, patch

from utils import Singleton
from exception.service_exception import ServiceException
from service.opensearch_service import OpensearchService
from tests.test_utils import TestUtils
from opensearchpy.exceptions import OpenSearchException

class TestOpensearchService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/opensearch/'


    def setUp(self):
        Singleton.clear_instance(OpensearchService)
        self.config = MagicMock()
        self.config.host = "test_host"
        self.config.port = 443
        self.config.pool_maxsize = 10
        self.config.index = "test_index"
        self.config.region = "us-east-1"
        self.config.service = "es"
        self.service = OpensearchService(self.config)


    def tearDown(self) -> None:
        del self.service
        Singleton.clear_instance(OpensearchService)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics(self, mock_search):
        """
        Tests whether this function correctly returns the total_executions and failed_executions count.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 126)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 6)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_with_non_existent_owner_id(self, mock_search):
        """
        Tests if the function correctly handles empty data for non existent owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_for_non_existent_owner_id_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_for_non_existent_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 0)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics_returns_failed_count_zero_for_status_error_from_open_search(self, mock_search):
        """
        Tests if the function correctly handles empty data for the status 'ERROR'.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_with_zero_failed_count_for_status_error_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 10)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics_with_no_executions_available_for_specified_date_range_in_open_search(self, mock_search):
        """
        Tests if the function correctly handles no executions available for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_with_no_executions_for_specified_date_range_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_executions_metrics_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 0)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics_opensearch_exception(self, mock_search):
        """
        Tests if the function raises a ServiceException when an OpenSearchException occurs.
        """
        mock_search.side_effect = OpenSearchException("OpenSearch error")
        with self.assertRaises(ServiceException):
            self.service.get_executions_metrics(owner_id="owner_id", start_date="2024-05-20T12:27:48.184Z", end_date="2024-06-20T12:27:48.184Z")


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date(self, mock_search):
        """
        Tests whether this function correctly returns count of failed_executions and total_executions by date.
        """
        owner_id = "owner_id"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_with_non_existent_owner_id(self, mock_search):
        """
        Tests if the function correctly handles empty data for non existent owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_for_non_existent_owner_id_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_for_non_existent_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_returns_failed_count_zero_for_status_error_from_open_search(self, mock_search):
        """
        Tests if the function correctly handles empty data for the status 'ERROR'.
        """
        owner_id = "owner_id"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_with_zero_failed_count_for_status_error_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"][0]["total_executions"]["value"], 10)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"][0]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_with_no_executions_available_for_specified_date_range_in_open_search(self, mock_search):
        """
        Tests if the function correctly handles no executions available for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_with_no_executions_for_specified_date_range_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_execution_metrics_by_date_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_opensearch_exception(self, mock_search):
        """
        Tests if the function raises a ServiceException when an OpenSearchException occurs.
        """
        mock_search.side_effect = OpenSearchException("OpenSearch error")
        with self.assertRaises(ServiceException):
            self.service.get_execution_metrics_by_date(owner_id="owner_id", start_date="2024-06-16T12:27:48.184Z", end_date="2024-06-20T12:27:48.184Z")


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations(self, mock_search):
        owner_id = "owner_id"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_with_non_existent_owner_id(self, mock_search):
        """
        Tests if the function correctly handles empty data for non existent owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_for_non_existent_owner_id_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_for_non_existent_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_returns_failed_executions_zero_for_status_error_from_open_search(self, mock_search):
        """
        Tests if the function correctly handles empty data for the status 'ERROR'.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_with_zero_failed_count_for_status_error_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"][0]["total_executions"]["value"], 10)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"][0]["failed_executions"]["unique_executions"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_with_no_executions_available_for_specified_date_range_in_open_search(self, mock_search):
        """
        Tests if the function correctly handles no executions available for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_with_no_executions_for_specified_date_range_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_integrations_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_opensearch_exception(self, mock_search):
        """
        Tests if the function raises a ServiceException when an OpenSearchException occurs.
        """
        mock_search.side_effect = OpenSearchException("OpenSearch error")
        with self.assertRaises(ServiceException):
            self.service.get_workflow_integrations(owner_id="owner_id", start_date="2024-05-20T08:19:24.908Z", end_date="2024-06-20T08:19:24.908Z")


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions(self, mock_search):
        """
        Tests whether this function correctly returns the failed executions of a workflow by date.
        """
        owner_id = "owner_id"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_with_non_existent_owner_id(self, mock_search):
        """
        Tests if the function correctly handles empty data for non existent owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_for_non_existent_owner_id_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_for_non_existent_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_with_no_executions_available_for_specified_date_range_in_open_search(self, mock_search):
        """
        Tests if the function correctly handles no executions available for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_with_no_executions_for_specified_date_range_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_returns_empty_data_for_status_error_from_open_search(self, mock_search):
        """
        Tests if the function correctly handles empty data for status 'ERROR'.
        """
        owner_id = "owner_id"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_with_empty_data_for_error_status_response.json"
        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_failed_executions_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_opensearch_exception(self, mock_search):
        """
        Tests if the function raises a ServiceException when an OpenSearchException occurs.
        """
        mock_search.side_effect = OpenSearchException("OpenSearch error")
        with self.assertRaises(ServiceException):
            self.service.get_workflow_failed_executions(owner_id="owner_id", start_date="2024-01-16T08:19:24.908Z", end_date="2024-06-20T08:19:24.908Z")


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failures_should_return_unique_failed_executions_of_each_workflow(self, mock_search):
        """
        Tests if the function returns unique failed executions of each workflow of specific owner within specified date range. Also verifies if query structure is correct.
        """
        owner_id = "owner_id"
        start_date = "2024-09-20T00:00:00.908Z"
        end_date = "2024-09-26T11:59:24.908Z"

        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_failures_query.json"
        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failures_query_response.json"

        mock_query = TestUtils.get_file_content(mock_query_path)
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failures(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_once_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failures_should_return_empty_data_for_no_failed_executions(self, mock_search):
        """
        Tests if the function correctly handles no failed executions available for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-09-20T00:00:00.908Z"
        end_date = "2024-09-26T11:59:24.908Z"

        mock_query_path = self.TEST_RESOURCE_PATH + "get_workflow_failures_query.json"
        mock_response_path = self.TEST_RESOURCE_PATH + "get_workflow_failures_query_empty_data_response.json"

        mock_query = TestUtils.get_file_content(mock_query_path)
        mock_response = TestUtils.get_file_content(mock_response_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failures(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_once_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["workflows"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failures_should_raise_service_exception_when_opensearch_exception_is_thrown(self, mock_search):
        """
        Tests if the function raises a ServiceException when an OpenSearchException occurs.
        """
        mock_search.side_effect = OpenSearchException("OpenSearch error")
        with self.assertRaises(ServiceException):
            self.service.get_workflow_failures(owner_id="owner_id", start_date="2024-01-16T08:19:24.908Z", end_date="2024-06-20T08:19:24.908Z")

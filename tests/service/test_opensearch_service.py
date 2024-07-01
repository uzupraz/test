import unittest
from unittest.mock import MagicMock, patch
from enums.status import ServiceStatus
from exception.service_exception import ServiceException
from service.opensearch_service import OpensearchService
from tests.test_utils import TestUtils
from opensearchpy.exceptions import OpenSearchException

class TestOpensearchService(unittest.TestCase):


    test_resource_path = '/tests/resources/opensearch/'


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

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 126)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 6)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics_no_data_owner_id(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_executions_metrics_no_data_owner_id_response.json"
        mock_query_path = self.test_resource_path + "get_executions_metrics_no_data_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 0)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics_no_data_status_error(self, mock_search):
        """
        Tests if the function correctly handles no data for the status ERROR.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_executions_metrics_no_data_status_error_response.json"
        mock_query_path = self.test_resource_path + "get_executions_metrics_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_executions_metrics(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["total_executions"]["value"], 10)
        self.assertEqual(response["aggregations"]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_executions_metrics_no_data_date_range(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_executions_metrics_no_data_date_range_response.json"
        mock_query_path = self.test_resource_path + "get_executions_metrics_query.json"

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

        mock_response_path = self.test_resource_path + "get_execution_metrics_by_date_response.json"
        mock_query_path = self.test_resource_path + "get_execution_metrics_by_date_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)
        
        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_no_data_owner_id(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_execution_metrics_by_date_no_data_owner_id_response.json"
        mock_query_path = self.test_resource_path + "get_execution_metrics_by_date_no_data_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_no_data_status_error(self, mock_search):
        """
        Tests if the function correctly handles no data for the status ERROR.
        """
        owner_id = "owner_id"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_execution_metrics_by_date_no_data_status_error_response.json"
        mock_query_path = self.test_resource_path + "get_execution_metrics_by_date_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_execution_metrics_by_date(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"][0]["total_executions"]["value"], 10)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"][0]["failed_executions"]["failed_count"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_execution_metrics_by_date_no_data_date_range(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-06-16T12:27:48.184Z"
        end_date = "2024-06-20T12:27:48.184Z"

        mock_response_path = self.test_resource_path + "get_execution_metrics_by_date_no_data_date_range_response.json"
        mock_query_path = self.test_resource_path + "get_execution_metrics_by_date_query.json"

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

        mock_response_path = self.test_resource_path + "get_workflow_integrations_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_integrations_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)
        
        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_no_data_owner_id(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_integrations_no_data_owner_id_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_integrations_no_data_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_no_data_status_error(self, mock_search):
        """
        Tests if the function correctly handles no data for the status ERROR.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_integrations_no_data_status_error_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_integrations_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_integrations(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"][0]["total_executions"]["value"], 10)
        self.assertEqual(response["aggregations"]["integrations"]["buckets"][0]["failed_executions"]["unique_executions"]["value"], 0)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_integrations_no_data_date_range(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-05-20T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_integrations_no_data_date_range_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_integrations_query.json"

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

        mock_response_path = self.test_resource_path + "get_workflow_failed_executions_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_failed_executions_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)
        
        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_no_data_owner_id(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified owner_id.
        """
        owner_id = "nonexistent_owner"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_failed_executions_no_data_owner_id_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_failed_executions_no_data_owner_id_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_no_data_date_range(self, mock_search):
        """
        Tests if the function correctly handles no data for the specified date range.
        """
        owner_id = "owner_id"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_failed_executions_no_data_date_range_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_failed_executions_query.json"

        mock_response = TestUtils.get_file_content(mock_response_path)
        mock_query = TestUtils.get_file_content(mock_query_path)

        mock_search.return_value = mock_response
        response = self.service.get_workflow_failed_executions(owner_id=owner_id, start_date=start_date, end_date=end_date)

        self.assertEqual(response, mock_response)
        mock_search.assert_called_with(body=mock_query, index=self.service.index)
        self.assertEqual(response["aggregations"]["by_date"]["buckets"], [])


    @patch("service.opensearch_service.OpenSearch.search")
    def test_get_workflow_failed_executions_no_error_status(self, mock_search):
        """
        Tests if the function correctly handles no data for status 'ERROR'.
        """
        owner_id = "owner_id"
        start_date = "2024-01-16T08:19:24.908Z"
        end_date = "2024-06-20T08:19:24.908Z"

        mock_response_path = self.test_resource_path + "get_workflow_failed_executions_no_error_status_response.json"
        mock_query_path = self.test_resource_path + "get_workflow_failed_executions_query.json"

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

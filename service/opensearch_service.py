from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, exceptions
import boto3

from utils import Singleton
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from configuration import OpensearchConfig
from model import WorkflowExecutionEvent


log = common_ctrl.log


class OpensearchService(metaclass=Singleton):


    def __init__(self, config: OpensearchConfig) -> None:
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, config.region, config.service)

        self.client = OpenSearch(
            hosts=[{'host': config.host, 'port': config.port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=config.pool_maxsize
        )
        self.index = config.index


    def _build_base_query(self, owner_id:str, start_date:str, end_date:str, is_external:bool=False) -> dict:
        return {
            "bool": {
                "filter": [
                    {"match_phrase": {"owner_id": owner_id}},
                    {"match_phrase": {"is_external": is_external}},
                    {
                        "range": {
                            "event_timestamp": {
                                "gte": start_date,
                                "lte": end_date,
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ]
            }
        }


    def _build_histogram_aggregation(self) -> dict:
        return {
            "by_date": {
                "date_histogram": {
                    "field": "event_timestamp",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "unique_executions": {
                        "cardinality": {
                            "field": "execution_id.keyword"
                        }
                    }
                }
            }
        }


    def _execute_query(self, query:dict, owner_id:str, start_date:str, end_date:str) -> dict:
        try:
            response = self.client.search(body=query, index=self.index)
            return response
        except exceptions as e:
            log.exception('Failed to search in opensearch. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(e.status_code if hasattr(e, 'status_code') else 500, ServiceStatus.FAILURE, str(e))
        except Exception as e:
            log.exception('Failed to search in opensearch. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(500, ServiceStatus.FAILURE, str(e))


    def get_fluent_executions_count(self, owner_id:str, start_date:str, end_date:str) -> int:
        """
        Fetches the count of fluent executions within the specified date range.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            int: The count of fluent executions.
        """
        log.info('Searching for the number of fluent executions. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)

        query = {
            "size": 0,
            "query": self._build_base_query(owner_id, start_date=start_date, end_date=end_date),
            "aggs": self._build_histogram_aggregation()
        }

        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        unique_executions_count = sum([bucket['unique_executions']['value'] for bucket in response['aggregations']['by_date']['buckets']])
        return unique_executions_count


    def get_failed_events_count(self, owner_id:str, start_date:str, end_date:str) -> int:
        """
        Fetches the count of failed events within the specified date range.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            int: The count of failed events.
        """
        log.info('Searching for the number of failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)

        query = self._build_base_query(owner_id, start_date=start_date, end_date=end_date)
        query['bool']['filter'].append({"match_phrase": {"status": "ERROR"}})

        query = {
            "size": 0,
            "query": query,
            "aggs": self._build_histogram_aggregation()
        }

        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        unique_failed_events_count = sum([bucket['unique_executions']['value'] for bucket in response['aggregations']['by_date']['buckets']])
        return unique_failed_events_count


    def get_execution_and_error_counts(self, owner_id:str, start_date:str, end_date:str) -> list[WorkflowExecutionEvent]:
        """
        Fetches the counts for fluent executions and failed events, aggregated by date.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            list[WorkflowExecutionEvent]: A list of WorkflowExecutionEvent containing date, fluent executions count, and failed events count.
        """
        log.info('Fetching counts for fluent executions and failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)

        fluent_executions_query = {
            "size": 0,
            "query": self._build_base_query(owner_id, start_date=start_date, end_date=end_date),
            "aggs": self._build_histogram_aggregation()
        }

        failed_events_query = {
            "size": 0,
            "query": self._build_base_query(owner_id=owner_id, start_date=start_date, end_date=end_date),
            "aggs": self._build_histogram_aggregation()
        }
        failed_events_query['query']['bool']['filter'].append({"match_phrase": {"status": "ERROR"}})

        # Fetch counts for fluent executions and failed events
        fluent_executions_counts = self.fetch_aggregated_data(query=fluent_executions_query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        failed_events_counts = self.fetch_aggregated_data(query=failed_events_query, owner_id=owner_id, start_date=start_date, end_date=end_date)

        # Combine the results based on date
        combined_results = []
        for date in fluent_executions_counts.keys():
            combined_results.append(
                WorkflowExecutionEvent(
                    date=date,
                    failed_events=failed_events_counts.get(date, 0),
                    fluent_executions=fluent_executions_counts.get(date, 0),
                )
            )
        return combined_results


    def fetch_aggregated_data(self, query:dict, owner_id:str, start_date:str, end_date:str) -> dict:
        """
        Fetches aggregated data based on the given query.

        Args:
            query (dict): The search query.
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            dict: Aggregated search results.
        """
        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        buckets = response['aggregations']['by_date']['buckets']
        results = {bucket['key_as_string']: bucket['unique_executions']['value'] for bucket in buckets}
        return results

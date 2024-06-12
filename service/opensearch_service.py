from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, exceptions
import boto3
from datetime import datetime, timedelta

from utils import Singleton
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus


log = common_ctrl.log


class OpensearchService(metaclass=Singleton):


    def __init__(self) -> None:
        host = ''  # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com
        region = 'us-west-2'
        service = 'es'
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)

        self.client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )


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
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "should": [
                                    {"match_phrase": {"owner_id": owner_id}}
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        {"match_phrase": {"is_external": False}},
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
        }
        try:
            response = self.client.search(body=query, index='python-test-index')
            return response['hits']['total']['value']
        except exceptions as e:
            log.exception('Failed to search for the number of fluent executions. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(e.status_code if hasattr(e, 'status_code') else 500, ServiceStatus.FAILURE, str(e))
        except Exception as e:
            log.exception('Failed to search for the number of fluent executions. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(500, ServiceStatus.FAILURE, str(e))


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
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "should": [
                                    {"match_phrase": {"owner_id": owner_id}}
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        {"match_phrase": {"is_external": False}},
                        {
                            "bool": {
                                "should": [
                                    {"match_phrase": {"status": "ERROR"}}
                                ],
                                "minimum_should_match": 1
                            }
                        },
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
        }
        try:
            response = self.client.search(body=query, index='python-test-index')
            return response['hits']['total']['value']
        except exceptions as e:
            log.exception('Failed to search for the number of failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(e.status_code if hasattr(e, 'status_code') else 500, ServiceStatus.FAILURE, str(e))
        except Exception as e:
            log.exception('Failed to search for the number of failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(500, ServiceStatus.FAILURE, str(e))


    def get_execution_and_error_counts(self, owner_id:str, start_date:str, end_date:str) -> list[dict]:
        """
        Fetches the counts for fluent executions and failed events, aggregated by date.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            list[dict]: A list of dictionaries containing date, fluent executions count, and failed events count.
        """
        log.info('Fetching counts for fluent executions and failed events. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
        
        fluent_executions_query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"match_phrase": {"owner_id": owner_id}},
                        {"match_phrase": {"is_external": False}},
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
            },
            "aggs": {
                "by_date": {
                    "date_histogram": {
                        "field": "event_timestamp",
                        "calendar_interval": "day",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        }

        failed_events_query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"match_phrase": {"owner_id": owner_id}},
                        {"match_phrase": {"is_external": False}},
                        {"match_phrase": {"status": "ERROR"}},
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
            },
            "aggs": {
                "by_date": {
                    "date_histogram": {
                        "field": "event_timestamp",
                        "calendar_interval": "day",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        }

        # Fetch counts for fluent executions and failed events
        fluent_executions_counts = self.fetch_aggregated_data('python-test-index', fluent_executions_query, owner_id, start_date, end_date)
        failed_events_counts = self.fetch_aggregated_data('python-test-index', failed_events_query, owner_id, start_date, end_date)

        # Generate complete date range
        all_dates = self.generate_date_range(datetime.fromisoformat(start_date), datetime.fromisoformat(end_date))

        # Combine the results based on date and filter out entries where both counts are 0
        combined_results = []
        for date in all_dates:
            fluent_count = fluent_executions_counts.get(date, 0)
            failed_count = failed_events_counts.get(date, 0)
            if fluent_count != 0 or failed_count != 0:
                combined_results.append({
                    "date": date,
                    "fluentExecutions": fluent_count,
                    "failedEvents": failed_count
                })
        return combined_results


    def fetch_aggregated_data(self, index_name:str, query:dict, owner_id:str, start_date:str, end_date:str) -> dict:
        """
        Fetches aggregated data based on the given query.

        Args:
            index_name (str): The name of the index to search.
            query (dict): The search query.
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            dict: Aggregated search results.
        """
        try:
            response = self.client.search(index=index_name, body=query)
            buckets = response['aggregations']['by_date']['buckets']
            results = {bucket['key_as_string']: bucket['doc_count'] for bucket in buckets}
            return results
        except exceptions as e:
            log.exception('Failed to fetch aggregated data. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(e.status_code if hasattr(e, 'status_code') else 500, ServiceStatus.FAILURE, str(e))
        except Exception as e:
            log.exception('Failed to fetch aggregated data. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(500, ServiceStatus.FAILURE, str(e))


    def generate_date_range(self, start_date: datetime, end_date: datetime) -> list:
        """
        Generates a list of dates between the start and end dates, inclusive.

        Args:
            start_date (datetime): The start date.
            end_date (datetime): The end date.

        Returns:
            list: A list of dates in the format 'YYYY-MM-DD'.
        """
        delta = timedelta(days=1)
        current_date = start_date
        date_list = []
        while current_date <= end_date:
            date_list.append(current_date.strftime("%Y-%m-%d"))
            current_date += delta
        return date_list

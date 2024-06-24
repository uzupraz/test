from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3

from utils import Singleton
from controller import common_controller as common_ctrl
from exception import ServiceException
from enums import ServiceStatus
from configuration import OpensearchConfig
from model import WorkflowExecutionMetric, WorkflowIntegration, WorkflowItem, WorkflowFailedEvent


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


    def get_executions_metrics(self, owner_id:str, start_date:str, end_date:str) -> dict:
        """
        Counts the total active workflows from DynamoDB. Also, counts the total workflows executions and failed executions having any status and return the number of unique executions as same executions are stored multiple times in opensearch with different status.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            int: Unique count of workflows executions.
        """
        log.info('Searching for the number of workflow fluent executions. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)

        query = self._build_base_query(owner_id, start_date=start_date, end_date=end_date)
        aggs = {
            "total_executions": {
                "cardinality": {"field": "execution_id"}
            },
            "failed_executions": {
                "filter": {"match_phrase": {"status": "ERROR"}},
                "aggs": {
                    "failed_count": {"cardinality": {"field": "execution_id"}}
                }
            }
        }
        query["aggs"] = aggs

        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        # Extract metrics from the response
        total_executions = response['aggregations']['total_executions']['value']
        failed_executions = response['aggregations']['failed_executions']['failed_count']['value']

        return {
            "total_executions": total_executions,
            "failed_executions": failed_executions
        }


    def get_execution_metrics_by_date(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowExecutionMetric]:
        """
        Fetches the counts for fluent executions and failed events, aggregated by date.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.

        Returns:
            list[WorkflowExecutionMetric]: A list of WorkflowExecutionMetric containing date, fluent executions count, and failed events count.
        """
        query = self._build_base_query(owner_id, start_date=start_date, end_date=end_date)
        aggs = {
            "by_date": {
                "date_histogram": {
                    "field": "event_timestamp",
                    "calendar_interval": "day",
                    "format": "yyyy-MM-dd"
                },
                "aggs": {
                    "total_executions": {
                        "cardinality": {
                            "field": "execution_id"
                        }
                    },
                    "failed_executions": {
                        "filter": {
                            "match_phrase": {
                                "status": "ERROR"
                            }
                        },
                        "aggs": {
                            "failed_count": {
                                "cardinality": {
                                    "field": "execution_id"
                                }
                            }
                        }
                    }
                }
            }
        }
        query["aggs"] = aggs

        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)

        metrics = [
            WorkflowExecutionMetric(
                date=bucket["key_as_string"],
                failed_executions=bucket["failed_executions"]["failed_count"]["value"],
                total_executions=bucket["total_executions"]["value"],
            )
            for bucket in response["aggregations"]["by_date"]["buckets"]
        ]
        return metrics


    def get_workflow_integrations(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowIntegration]:
        """
        Fetches the active workflow integrations from OpenSearch.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.
        
        Returns:
            list[WorkflowIntegration]: A list of active workflow integrations.
        """
        query = self._build_base_query(owner_id, start_date=start_date, end_date=end_date)
        aggs = {
            "integrations": {
                "terms": {"field": "workflow_id"},
                "aggs": {
                    "workflow_name": {
                        "terms": {"field": "workflow_name"},
                    },
                    "last_event_date": {
                        "max": {"field": "event_timestamp"},
                    },
                    "failed_executions": {
                        "filter": {
                            "term": {"status": "ERROR"},
                        },
                        "aggs": {
                            "unique_executions": {
                                "cardinality": {
                                    "field": "execution_id",
                                }
                            }
                        },
                    },
                    "total_executions": {
                        "cardinality": {"field": "execution_id"},
                    },
                },
            }
        }
        query["aggs"] = aggs
        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        workflow_integrations = [
            self._map_bucket_to_workflow_integration(bucket)
            for bucket in response["aggregations"]["integrations"]["buckets"]
        ]
        return workflow_integrations


    def get_workflow_failed_executions(self, owner_id: str, start_date: str, end_date: str) -> list[WorkflowFailedEvent]:
        """
        Fetches the failed workflow executions from OpenSearch.

        Args:
            owner_id (str): The owner ID.
            start_date (str): The start date in ISO format.
            end_date (str): The end date in ISO format.
        
        Returns:
            list[WorkflowFailedEvent]: A list of failed workflow executions.
        """
        query = self._build_base_query(owner_id, start_date=start_date, end_date=end_date)
        aggs = {
            "by_date": {
                "date_histogram": {
                    "field": "event_timestamp",
                    "interval": "day",
                    "format": "yyyy-MM-dd",
                },
                "aggs": {
                    "failed_executions": {
                        "terms": {"field": "event_id"},
                        "aggs": {
                            "workflow_id": {
                                "terms": {"field": "workflow_id"},
                            },
                            "workflow_name": {
                                "terms": {"field": "workflow_name"},
                            },
                            "error_code": {
                                "terms": {"field": "error_code"},
                            },
                        },
                    }
                },
            }
        }

        query["query"]["bool"]["filter"].insert(0, {"match_phrase": {"status": "ERROR"}})
        query["aggs"] = aggs

        response = self._execute_query(query=query, owner_id=owner_id, start_date=start_date, end_date=end_date)
        return self._map_workflow_failed_executions_response(response)


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
                event_id = nested_bucket["key"]
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


    def _map_bucket_to_workflow_integration(self, bucket: dict) -> WorkflowIntegration:
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


    def _build_base_query(self, owner_id:str, start_date:str=None, end_date:str=None, is_external:bool=False, from_:int=0, size:int=0) -> dict:
        base_query = {
            "from": from_,
            "size": size,
            "query": {
                "bool": {
                    "filter": [
                        {"match_phrase": {"owner_id": owner_id}},
                        {"match_phrase": {"is_external": is_external}}
                    ]
                }
            }
        }

        if start_date is not None and end_date is not None:
            base_query["query"]["bool"]["filter"].append({
                "range": {
                    "event_timestamp": {
                        "gte": start_date,
                        "lte": end_date,
                        "format": "strict_date_optional_time"
                    }
                }
            })

        return base_query


    def _execute_query(self, query:dict, owner_id:str, start_date:str, end_date:str) -> dict:
        try:
            response = self.client.search(body=query, index=self.index)
            return response
        except Exception:
            log.exception('Failed to search in opensearch. owner_id: %s, start_date: %s, end_date: %s', owner_id, start_date, end_date)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Could not search data')

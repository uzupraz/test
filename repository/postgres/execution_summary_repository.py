from configuration import PostgresConfig
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus
from model import WorkflowExecutionMetric, WorkflowIntegration, WorkflowStats, WorkflowItem, WorkflowFailedEvent, WorkflowErrorFlatStructure

import logging as log
from typing import Dict, List
from psycopg2.pool import SimpleConnectionPool


class ExecutionSummaryRepository(metaclass=Singleton):
    

    def __init__(self, postgres_config: PostgresConfig):
        """
        Initializes the repository with PostgreSQL configuration and creates a connection pool.

        Args:
            postgres_config (PostgresConfig): Configuration for PostgreSQL database.
        """
        self.postgres_config = postgres_config
        self.pool = self._initialize_connection_pool()


    def _initialize_connection_pool(self):
        """
        Initializes a connection pool with the provided PostgreSQL configuration.
        Ensures efficient reuse of database connections.
        """
        try:
            return SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=self._construct_connection_string()
            )
        except Exception:
            log.exception("Failed to initialize connection pool.")
            raise


    def get_execution_stats(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> WorkflowStats:
        """
        Retrieves summary statistics for workflow executions within a specific time range.
        
        Args:
            owner_id (str): Unique identifier of the owner whose workflows are queried.
            start_timestamp (int): Start of the time range (Unix timestamp).
            end_timestamp (int): End of the time range (Unix timestamp).

        Returns:
            WorkflowStats: An object containing execution statistics (total and failed executions).

        Raises:
            ServiceException: If the query fails or an error occurs while retrieving data.
        """
        log.info("Retrieving execution stats. owner_id: %s", owner_id)

        query = """SELECT 
            COUNT(*) AS total_executions,
            COUNT(CASE WHEN status = 'ERROR' THEN 1 END) AS failed_executions
        FROM 
            interconnecthub_executions_summary
        WHERE
            owner_id = %s
            AND event_timestamp BETWEEN %s AND %s;"""
        
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (owner_id, start_timestamp, end_timestamp))
            stats = cursor.fetchone()
            log.info("Execution stats retrieved successfully. owner_id: %s", owner_id)
            return WorkflowStats(active_workflows_count=0, total_executions_count=stats[0], failed_executions_count=stats[1])
        except Exception as e:
            log.exception('Unable to retrieve execution stats. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Unable to retrieve execution stats.')
        finally:
            self._close_cursor_and_connection(cursor, conn)


    def get_workflow_execution_metrics_by_date(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> List[WorkflowExecutionMetric]:
        """
        Retrieves workflow execution metrics grouped by date.

        Args:
            owner_id (str): Unique identifier of the owner whose workflows are queried.
            start_timestamp (int): Start of the time range (Unix timestamp).
            end_timestamp (int): End of the time range (Unix timestamp).

        Returns:
            List[WorkflowExecutionMetric]: A list of metrics for each day, including total and failed executions.

        Raises:
            ServiceException: If the query fails or an error occurs while retrieving data.
        """
        log.info("Retrieving execution metrics. owner_id: %s", owner_id)

        query = """
        SELECT 
            TO_TIMESTAMP(event_timestamp)::DATE as execution_date,
            COUNT(*) AS total_executions,
            COUNT(CASE WHEN status = 'ERROR' THEN 1 END) AS failed_executions
        FROM 
            interconnecthub_executions_summary
        WHERE 
            owner_id = %s 
            AND event_timestamp BETWEEN %s AND %s
        GROUP BY 
            execution_date
        ORDER BY 
            execution_date;"""
        
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (owner_id, start_timestamp, end_timestamp))
            metrics = cursor.fetchall()
            log.info("Execution metrics retrieved successfully. owner_id: %s", owner_id)
            return [WorkflowExecutionMetric(date=metric[0].strftime('%Y-%m-%d'), total_executions=metric[1], failed_executions=metric[2]) for metric in metrics]
        except Exception as e:
            log.exception('Unable to retrieve execution metrics. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Unable to retrieve execution metrics.')
        finally:
            self._close_cursor_and_connection(cursor, conn)


    def get_workflow_integrations(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> List[WorkflowIntegration]:
        """
        Retrieves workflow integrations with execution statistics.

        Args:
            owner_id (str): Unique identifier of the owner whose workflows are queried.
            start_timestamp (int): Start of the time range (Unix timestamp).
            end_timestamp (int): End of the time range (Unix timestamp).

        Returns:
            List[WorkflowIntegration]: A list of workflow integrations with detailed execution statistics.

        Raises:
            ServiceException: If the query fails or an error occurs while retrieving data.
        """
        log.info("Retrieving workflow integrations. owner_id: %s", owner_id)

        query = """
        WITH workflow_stats AS (
            SELECT 
                workflow_id,
                workflow_name,
                MAX(event_timestamp) as last_event_timestamp,
                COUNT(*) as total_executions,
                COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as failed_executions
            FROM interconnecthub_executions_summary
            WHERE owner_id = %s
            AND event_timestamp BETWEEN %s AND %s
            GROUP BY workflow_id, workflow_name
        )
        SELECT 
            workflow_id,
            workflow_name,
            TO_TIMESTAMP(last_event_timestamp) as last_event_date,
            failed_executions,
            total_executions
        FROM workflow_stats
        ORDER BY last_event_timestamp DESC;
        """
        
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (owner_id, start_timestamp, end_timestamp))
            integrations = cursor.fetchall()
            log.info("Workflow intergrations retrieved successfully. owner_id: %s", owner_id)
            return [
                WorkflowIntegration(
                    workflow=WorkflowItem(id=integartion[0], name=integartion[1]),
                    last_event_date=integartion[2].strftime('%Y-%m-%d'),
                    failed_executions_count=integartion[3],
                    total_executions_count=integartion[4],
                    failed_executions_ratio=(
                        float(integartion[3]) / float(integartion[4]) if integartion[4] > 0 else 0.0
                    )
                )
                for integartion in integrations
            ]
        except Exception:
            log.exception('Unable to retrieve workflow integrations. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Unable to retrieve workflow integrations.')
        finally:
            self._close_cursor_and_connection(cursor, conn)


    def get_workflow_failed_executions(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> List[WorkflowExecutionMetric]:
        """
        Retrieves detailed information about failed workflow executions.

        Args:
            owner_id (str): Unique identifier of the owner whose workflows are queried.
            start_timestamp (int): Start of the time range (Unix timestamp).
            end_timestamp (int): End of the time range (Unix timestamp).

        Returns:
            List[WorkflowExecutionMetric]: A list of failed execution details.

        Raises:
            ServiceException: If the query fails or an error occurs while retrieving data.
        """
        log.info("Retrieving workflow failed executions. owner_id: %s", owner_id)

        query = """
        SELECT DISTINCT
            execution_id,
            event_id,
            TO_TIMESTAMP(event_timestamp) as event_datetime,
            workflow_name,
            workflow_id
        FROM interconnecthub_executions_summary
        WHERE owner_id = %s
        AND event_timestamp BETWEEN %s AND %s
        AND status = 'ERROR'
        ORDER BY event_datetime DESC;
        """
        
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (owner_id, start_timestamp, end_timestamp))
            failed_executions = cursor.fetchall()
            log.info("Workflow failed executions retrieved successfully. owner_id: %s", owner_id)
            return [
                WorkflowFailedEvent(
                    execution_id=execution[0],
                    event_id=execution[1],
                    date=execution[2].strftime('%Y-%m-%d'),
                    workflow=WorkflowItem(id=execution[4], name=execution[3]),
                    error_code=None
                )
                for execution in failed_executions
            ]
        except Exception:
            log.exception('Unable to retrieve workflow failed executions. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Unable to retrieve workflow failed executions.')
        finally:
            self._close_cursor_and_connection(cursor, conn)


    def get_workflow_failures(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> List[WorkflowErrorFlatStructure]:
        """
        Retrieves the most common errors for workflows within a time range.

        Args:
            owner_id (str): Unique identifier of the owner whose workflows are queried.
            start_timestamp (int): Start of the time range (Unix timestamp).
            end_timestamp (int): End of the time range (Unix timestamp).

        Returns:
            List[WorkflowErrorFlatStructure]: A list of workflow errors with their occurrence count.

        Raises:
            ServiceException: If the query fails or an error occurs while retrieving data.
        """
        log.info("Retrieving workflow failures. owner_id: %s", owner_id)

        query = """
        SELECT 
            workflow_id,
            workflow_name,
            error_code,
            COUNT(*) as error_occurrence
        FROM interconnecthub_executions_summary
        WHERE owner_id = %s
        AND event_timestamp BETWEEN %s AND %s
        AND status = 'ERROR'
        GROUP BY workflow_id, workflow_name, error_code
        ORDER BY error_occurrence DESC;
        """
        
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (owner_id, start_timestamp, end_timestamp))
            failures = cursor.fetchall()
            log.info("Workflow failures retrieved successfully. owner_id: %s", owner_id)
            return [WorkflowErrorFlatStructure(workflow_id=failure[0], workflow_name=failure[1], error_code=failure[2], error_occurrence=failure[3]) for failure in failures]
        except Exception:
            log.exception('Unable to retrieve workflow failures. owner_id: %s', owner_id)
            raise ServiceException(500, ServiceStatus.FAILURE, 'Unable to retrieve workflow failures.')
        finally:
            self._close_cursor_and_connection(cursor, conn)


    def _close_cursor_and_connection(self, cursor, conn):
        if cursor:
            cursor.close()
        if conn:
            self._release_connection(conn)


    def _get_connection(self):
        """
        Retrieves a connection from the pool.

        Returns:
            psycopg2.extensions.connection: A connection object from the pool.

        Raises:
            Exception: If the connection pool is not initialized.
        """
        if not self.pool:
            log.error("Cannot initialize connection pool.")
            raise Exception("Connection pool is not initialized.")
        return self.pool.getconn()


    def _release_connection(self, conn):
        """
        Releases a connection back to the pool.

        Args:
            conn (psycopg2.extensions.connection): The connection to be released.
        """
        if self.pool and conn:
            self.pool.putconn(conn)


    def _construct_connection_string(self):
        """
        Constructs a PostgreSQL connection string using the provided configuration.

        Returns:
            str: The PostgreSQL connection string.
        """
        return f"postgres://{self.postgres_config.postgres_user}:{self.postgres_config.postgres_pass}@{self.postgres_config.postgres_host}:{self.postgres_config.postgres_port}/{self.postgres_config.postgres_database}?sslmode=require"
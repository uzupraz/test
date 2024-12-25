from configuration import PostgresConfig
from utils import Singleton
from exception import ServiceException
from enums import ServiceStatus
from model import WorkflowExecutionMetric

import logging as log
from typing import Tuple, List
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


    def get_execution_stats(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> Tuple[int, int]:
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
            log.info("Execution stats retrieve successfully. owner_id: %s", owner_id)
            return stats
        except Exception as e:
            log.exception('Unable to retrieve execution stats. owner_id: %s', owner_id)
            raise ServiceException(409, ServiceStatus.FAILURE, 'Unable to retrieve execution stats.')
        finally:
            if cursor:
                cursor.close()
            if conn:
                self._release_connection(conn)


    def get_workflow_execution_metrics_by_date(self, owner_id: str, start_timestamp: int, end_timestamp: int) -> List[WorkflowExecutionMetric]:
        log.info("Retrieving execution metrics. owner_id: %s", owner_id)

        query = """SELECT 
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
            log.info("Execution metrics retrieve successfully. owner_id: %s", owner_id)
            return [WorkflowExecutionMetric(date=metric[0].isoformat(), total_executions=metric[1], failed_executions=metric[2]) for metric in metrics]
        except Exception as e:
            log.exception('Unable to retrieve execution metrics. owner_id: %s', owner_id)
            raise ServiceException(409, ServiceStatus.FAILURE, 'Unable to retrieve execution metrics.')
        finally:
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
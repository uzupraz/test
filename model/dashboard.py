from dataclasses import dataclass

from enums import SystemStatus, WorkflowErrorCode, WorkflowErrorSeverity

@dataclass
class WorkflowItem:
    id: str
    name: str

@dataclass
class WorkflowStats:
    active_workflows_count: int
    failed_executions_count: int
    total_executions_count: int
    system_status: str = SystemStatus.ONLINE.value

@dataclass
class WorkflowIntegration:
    failed_executions_count: int
    total_executions_count: int
    failed_executions_ratio: float
    last_event_date: str
    workflow: WorkflowItem

@dataclass
class WorkflowError:
    occurrence: int
    error_code: str = WorkflowErrorCode.UNKNOWN.value
    severity: str = WorkflowErrorSeverity.HIGH.value


@dataclass
class WorkflowFailure:
    workflow: WorkflowItem
    errors: list[WorkflowError]


@dataclass
class WorkflowErrorFlatStructure:
    error_occurrence: int
    workflow_name: str
    workflow_id: str
    error_code: str = WorkflowErrorCode.UNKNOWN.value


@dataclass
class WorkflowFailedEvent:
    date: str
    error_code: str
    event_id: str
    execution_id: str
    workflow: WorkflowItem

@dataclass
class WorkflowExecutionMetric:
    date: str
    failed_executions: int
    total_executions: int

from dataclasses import dataclass

@dataclass
class WorkflowItem:
    id: str
    name: str

@dataclass
class WorkflowStats:
    active_workflows: int
    failed_events: int
    fluent_executions: int
    system_status: str = "Online"

@dataclass
class WorkflowIntegration:
    failure_count: int
    failure_ratio: float
    last_event_date: str
    workflow: WorkflowItem

@dataclass
class WorkflowFailureItem:
    error_code: str
    failure_ratio: float
    severity: float

@dataclass
class WorkflowFailure:
    color: str
    workflow_name: str
    failures: list[WorkflowFailureItem]

@dataclass
class WorkflowFailedEvent:
    date: str
    error_code: str
    event_id: str
    workflow: WorkflowItem

@dataclass
class WorkflowExecutionEvent:
    date: str
    failed_events: int
    fluent_executions: int

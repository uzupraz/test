from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from dacite import from_dict


@dataclass
class Connection:
    source_node: str
    target_node: str


@dataclass
class Node:
    id: str
    name: str
    description: str
    type: str
    has_subflow: bool
    parameters: Dict[str, Any]
    node_template_id: str
    sub_workflow: Optional['SubWorkflow'] = field(default=None)


@dataclass
class Config:
    start_at: str = field(default=None)
    connections: List[Connection] = field(default=None)
    nodes: List[Node] = field(default=None)


@dataclass
class SubWorkflow:
    config: Config = field(default=None)


@dataclass
class Workflow:
    owner_id: str
    workflow_id: str
    event_name: str
    created_by: str
    created_by_name: str
    state: str
    version: int
    is_sync_execution: bool
    state_machine_arn: str
    is_binary_event: bool
    mapping_id: str | None = field(default=None)


    @classmethod
    def parse_from(cls, data: Dict[str, Any]) -> 'Workflow':
        return from_dict(data_class=Workflow, data=data)
    

    @classmethod
    def from_dict(cls, data:dict) -> 'Workflow':
        mapped_data = {
            "owner_id": data["ownerId"],
            "workflow_id": data["workflowId"],
            "event_name": data["event_name"],
            "created_by": data["createdBy"],
            "created_by_name": data["createdByName"],
            "state": data["state"],
            "version": data["version"],
            "is_sync_execution": data["is_sync_execution"],
            "state_machine_arn": data["state_machine_arn"],
            "is_binary_event": data["is_binary_event"],
            "mapping_id": data["mapping_id"]
        }
        return cls(**mapped_data)
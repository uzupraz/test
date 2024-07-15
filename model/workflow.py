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

    @classmethod
    def from_dict(cls, data:dict) -> 'Config':
        mapped_data = {
            "start_at": data.get("startAt"),
            "connections": [Connection(**connection) for connection in data.get("connections", [])],
            "nodes": [Node(**node) for node in data.get("nodes", [])]
        }
        return cls(**mapped_data)


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
    last_updated: str
    state: str
    version: int
    is_sync_execution: bool
    state_machine_arn: str
    is_binary_event: bool
    mapping_id: str
    config: Config


    @classmethod
    def parse_from(cls, data: Dict[str, Any]) -> 'Workflow':
        return from_dict(data_class=Workflow, data=data)
    

    @classmethod
    def from_dict(cls, data:dict) -> 'Workflow':
        mapped_data = {
            "owner_id": data.get("ownerId"),
            "workflow_id": data.get("workflowId"),
            "event_name": data.get("event_name"),
            "created_by": data.get("createdBy"),
            "created_by_name": data.get("createdByName"),
            "last_updated": data.get("lastUpdated"),
            "state": data.get("state"),
            "version": data.get("version"),
            "is_sync_execution": data.get("is_sync_execution"),
            "state_machine_arn": data.get("state_machine_arn"),
            "is_binary_event": data.get("is_binary_event"),
            "mapping_id": data.get("mapping_id"),
            "config": Config.from_dict(data.get("config"))
        }
        return cls(**mapped_data)
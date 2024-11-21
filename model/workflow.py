from datetime import datetime
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
    name: str
    event_name: str
    created_by: str
    created_by_name: str
    group_name: str
    state: str
    version: int
    is_sync_execution: bool
    state_machine_arn: str
    is_binary_event: bool
    creation_date: str = field(default_factory=lambda: datetime.now().isoformat())


    @classmethod
    def parse_from(cls, data: Dict[str, Any]) -> 'Workflow':
        return from_dict(data_class=Workflow, data=data)
    

    @classmethod
    def from_dict(cls, data:dict) -> 'Workflow':
        mapped_data = {
            "owner_id": data["ownerId"],
            "workflow_id": data["workflowId"],
            "name": data["name"],
            "event_name": data["event_name"],
            "created_by": data["createdBy"],
            "created_by_name": data["createdByName"],
            "group_name": data["groupName"],
            "state": data["state"],
            "version": data["version"],
            "is_sync_execution": data["is_sync_execution"],
            "state_machine_arn": data["state_machine_arn"],
            "is_binary_event": data["is_binary_event"],
            "creation_date": data["creationDate"],
        }
        return cls(**mapped_data)
    

    def as_dict(self) -> dict:
        return {
            "ownerId": self.owner_id,
            "workflowId": self.workflow_id,
            "name": self.name,
            "event_name": self.event_name,
            "createdBy": self.created_by,
            "createdByName": self.created_by_name,
            "groupName": self.group_name,
            "state": self.state,
            "version": self.version,
            "is_sync_execution": self.is_sync_execution,
            "state_machine_arn": self.state_machine_arn,
            "is_binary_event": self.is_binary_event,
            "creationDate": self.creation_date,
        }
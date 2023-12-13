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
    config: Config
    created_by: str
    created_by_name: str
    creation_date: str
    group_name: str
    name: str
    state: str
    workflow_version: int
    schema_version: int

    @classmethod
    def parse_from(cls, data: Dict[str, Any]) -> 'Workflow':
        return from_dict(data_class=Workflow, data=data)

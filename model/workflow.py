from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from dacite import from_dict


@dataclass
class Connection:

    sourceNode: str
    targetNode: str


@dataclass
class Node:

    id: str
    name: str
    description: str
    type: str
    hasSubflow: bool
    parameters: Dict[str, Any]
    nodeTemplateId: str
    subWorkflow: Optional['SubWorkflow'] = field(default=None)

@dataclass
class Config:

    startAt: str = field(default=None)
    connections: List[Connection] = field(default=None)
    nodes: List[Node] = field(default=None)


@dataclass
class SubWorkflow:

    config: Config = field(default=None)


@dataclass
class Workflow:

    ownerId: str
    workflowId: str
    config: Config
    createdBy: str
    createdByName: str
    creationDate: str
    groupName: str
    name: str
    state: str
    workflowVersion: int
    schemaVersion: int

    @classmethod
    def parse_from(cls, data: Dict[str, Any]) -> 'Workflow':
        return from_dict(data_class=Workflow, data=data)

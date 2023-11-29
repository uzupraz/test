from dataclasses import dataclass
from typing import List, Dict, Any, Optional


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
    subWorkflow: Optional['SubWorkflow']


@dataclass
class Config:

    startAt: str
    connections: List[Connection]
    nodes: List[Node]


@dataclass
class SubWorkflow:

    config: Config

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SubWorkflow':
        data['config'] = Config(**data['config'])
        return cls(**data)


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
    def from_dict(cls, data: Dict[str, Any]) -> 'Workflow':
        data['config']['connections'] = [Connection(**conn) for conn in data['config']['connections']]
        data['config']['nodes'] = [Node(**node) if not node['hasSubflow'] else Node(subWorkflow=SubWorkflow.from_dict(node['subWorkflow']), **node) for node in data['config']['nodes']]
        data['config'] = Config(**data['config'])
        return cls(**data)

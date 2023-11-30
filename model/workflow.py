from dataclasses import dataclass, field
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
    subWorkflow: Optional['SubWorkflow'] = None


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
        # Convert connections
        connections = []
        for conn in data['config']['connections']:
            connections.append(Connection(**conn))
        data['config']['connections'] = connections

        # Convert nodes
        nodes = []
        for node in data['config']['nodes']:
            if not node['hasSubflow']:
                nodes.append(Node(**node))
            else:
                # If it has a subWorkflow, remove the subWorkflow key from the node dictionary,
                # and create a SubWorkflow object using the from_dict method,
                # and creates a Node object with subWorkflow set to the created SubWorkflow object.
                sub_workflow = node.pop('subWorkflow')
                nodes.append(Node(subWorkflow=SubWorkflow.from_dict(sub_workflow), **node))
        data['config']['nodes'] = nodes

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
        # Convert connections
        connections = []
        for conn in data['config']['connections']:
            connections.append(Connection(**conn))
        data['config']['connections'] = connections

        # Convert nodes
        nodes = []
        for node in data['config']['nodes']:
            if not node['hasSubflow']:
                nodes.append(Node(**node))
            else:
                # If it has a subWorkflow, remove the subWorkflow key from the node dictionary,
                # and create a SubWorkflow object using the from_dict method,
                # and creates a Node object with subWorkflow set to the created SubWorkflow object.
                sub_workflow = node.pop('subWorkflow')
                nodes.append(Node(subWorkflow=SubWorkflow.from_dict(sub_workflow), **node))
        data['config']['nodes'] = nodes

        data['config'] = Config(**data['config'])
        return cls(**data)

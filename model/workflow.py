from typing import List, Dict, Any, Optional


class Connection:


    def __init__(self, sourceNode: str, targetNode: str) -> None:
        self.sourceNode = sourceNode
        self.targetNode = targetNode


class Node:


    def __init__(self, id: str, name: str, description: str, type: str, hasSubflow: bool, parameters: Dict[str, Any], nodeTemplateId: str, subWorkflow: Optional['SubWorkflow']) -> None:
        self.id = id
        self.name = name
        self.description = description
        self.type = type
        self.hasSubflow = hasSubflow
        self.parameters = parameters
        self.nodeTemplateId = nodeTemplateId
        self.subWorkflow = subWorkflow


class Config:


    def __init__(self, startAt: str, connections: List[Connection], nodes: List[Node]) -> None:
        self.startAt = startAt
        self.connections = connections
        self.nodes = nodes


class SubWorkflow:


    def __init__(self, config: Config) -> None:
        self.config = config


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SubWorkflow':
        data['config'] = Config(**data['config'])
        return cls(**data)


class Workflow:


    def __init__(self, ownerId: str, workflowId: str, config: Config, createdBy: str, createdByName: str, creationDate: str, groupName: str, name: str, state: str, workflowVersion: int, schemaVersion: int) -> None:
        self.ownerId = ownerId
        self.workflowId = workflowId
        self.config = config
        self.createdBy = createdBy
        self.createdByName = createdByName
        self.creationDate = creationDate
        self.groupName = groupName
        self.name = name
        self.state = state
        self.workflowVersion = workflowVersion
        self.schemaVersion = schemaVersion


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Workflow':
        data['config']['connections'] = [Connection(**conn) for conn in data['config']['connections']]
        data['config']['nodes'] = [Node(**node) if not node['hasSubflow'] else Node(subWorkflow=SubWorkflow.from_dict(node['subWorkflow']), **node) for node in data['config']['nodes']]
        data['config'] = Config(**data['config'])
        return cls(**data)

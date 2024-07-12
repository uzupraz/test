from dataclasses import dataclass


@dataclass
class OutputSchemaField:
    name: str
    type: str
    subtype: str | None
    operation: str
    mapped_to: str
    fields: list['OutputSchemaField'] | None


@dataclass
class OutputSchema:
    name: str
    type: str
    subtype: str | None
    fields: list[OutputSchemaField]


@dataclass
class InputSchema:
    name: str
    type: str
    subtype: str | None
    fields: list['InputSchema'] | None


@dataclass
class MappingFrom:
    format: str
    parameters: dict


@dataclass
class MappingTo:
    format: str
    parameters: dict


@dataclass
class Mapping:
    mapping_id: str
    from_ : MappingFrom
    to: MappingTo
    output_schema: OutputSchema
    input_schema: InputSchema


@dataclass
class DataStudioWorkflow:
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


    @classmethod
    def from_dict(cls, data:dict) -> 'DataStudioWorkflow':
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
            "mapping_id": data.get("mapping_id")
        }
        return cls(**mapped_data)

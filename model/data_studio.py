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
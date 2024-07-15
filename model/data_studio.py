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

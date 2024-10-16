import time

from dataclasses import dataclass, field
from typing import Optional, Dict

from enums import DataStudioMappingStatus


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


### Mapping
@dataclass
class DataStudioMapping:
    owner_id: str
    mapping_id: str
    revision: int
    status: DataStudioMappingStatus
    active: bool
    created_by: str

    name: Optional[str]
    description: Optional[str]
    sources: Optional[Dict]
    output: Optional[Dict]
    mapping: Optional[Dict]
    published_by: Optional[str]
    published_at: Optional[int]

    created_at: int = field(default=int(time.time()))
    
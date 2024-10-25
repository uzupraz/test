import time

from dataclasses import dataclass, field
from typing import Optional, Dict, List

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


@dataclass
class DataStudioMapping:
    id: str
    revision: str
    owner_id: str
    created_by: str
    status: str = DataStudioMappingStatus.DRAFT.value
    active: bool = False
    name: Optional[str] = None
    description: Optional[str] = None
    sources: Optional[Dict] = None
    output: Optional[Dict] = None
    mapping: Optional[Dict] = None
    published_by: Optional[str] = None
    published_at: Optional[int] = None
    version: Optional[str] = None
    tags: Optional[str] = None
    created_at: int = field(default_factory=lambda: int(time.time()))
 

@dataclass
class DataStudioMappingResponse:
    draft: Optional[DataStudioMapping]
    revisions: List[DataStudioMapping]

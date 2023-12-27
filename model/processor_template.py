from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class InputDescription:
    description: str
    format: str
    media_type: str


@dataclass
class OutputDescription:
    description: str
    format: str
    media_type: str


@dataclass
class ParameterDescription:
    description: str
    name: str
    order: int
    type: str
    required: bool


@dataclass
class ProcessorTemplate:
    template_id: str
    name: str
    description: str
    icon: str
    limit: int
    input: Optional[InputDescription]
    output: Optional[OutputDescription]
    parameter_editor: str
    parameters: Dict[str, ParameterDescription]
    processor_type: str
    version: int
    lambda_resource: str

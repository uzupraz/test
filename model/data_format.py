from dataclasses import dataclass
from typing import  Optional, Dict

@dataclass
class DataFormatProperties:
    lambda_arn: str
    parameters: dict

@dataclass
class DataFormat:
    format_name: str
    parser: DataFormatProperties
    writer: DataFormatProperties
    input_schema: Optional[Dict] = None
    output_schema: Optional[Dict] = None
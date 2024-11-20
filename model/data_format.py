from dataclasses import dataclass

@dataclass
class DataFormatProperties:
    lambda_arn: str
    parameters: dict

@dataclass
class DataFormat:
    format_name: str
    parser: DataFormatProperties
    writer: DataFormatProperties
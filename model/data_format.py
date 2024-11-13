from dataclasses import dataclass


@dataclass
class DataFormat:
    format_id: str
    name: str
    parser: dict
    writer: dict
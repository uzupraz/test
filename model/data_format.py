from dataclasses import dataclass


@dataclass
class DataFormat:
    format_id: str
    name: str
    parser: dict[str, dict]
    writer: dict[str, dict]
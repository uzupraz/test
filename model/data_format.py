from dataclasses import dataclass


@dataclass
class DataFormat:
    id: str
    parser: dict[str, dict]
    writer: dict[str, dict]
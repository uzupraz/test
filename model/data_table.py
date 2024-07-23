from dataclasses import dataclass, field

@dataclass
class ListTableResponse:
    name: str
    id: str
    size: float


@dataclass
class CustomerTableInfo:
    owner_id: str
    table_id: str
    table_name: str
    original_table_name: str
    description: str = field(default=None)
    created_by: str = field(default=None)
    creation_time: str = field(default=None)
    total_indexes: int = field(default=None)

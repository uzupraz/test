from dataclasses import dataclass

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
    description: str
    created_by: str
    creation_time: str
    total_indexes: int


    @classmethod
    def from_dict(cls, data:dict) -> 'CustomerTableInfo':
        mapped_data = {
            'owner_id': data.get('owner_id'),
            'table_id': data.get('table_id'),
            'table_name': data.get('table_name', ''),
            'original_table_name': data.get('original_table_name', ''),
            'description': data.get('description', ''),
            'created_by': data.get('created_by', ''),
            'creation_time': data.get('creation_time', ''),
            'total_indexes': data.get('total_indexes', '')
        }
        return cls(**mapped_data)

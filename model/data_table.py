from dataclasses import dataclass, field
from typing import List
from decimal import Decimal

from enums import TableStatus, IndexStatus, AutoBackupStatus, Backup

@dataclass
class ListTableResponse:
    name: str
    id: str
    size: Decimal


@dataclass
class IndexInfo:
    name: str
    partition_key: str
    sort_key: str | None = field(default=None)
    status: str = field(default=IndexStatus.ACTIVE.value)
    size: Decimal = field(default=0)
    item_count: Decimal = field(default=0)


@dataclass
class CustomerTableInfo:
    owner_id: str
    table_id: str
    table_name: str
    original_table_name: str
    partition_key: str
    sort_key: str | None = field(default=None)
    description: str | None = field(default=None)
    created_by: str | None = field(default=None)
    creation_time: str | None = field(default=None)
    total_indexes: Decimal = field(default=0)
    read_capacity_units: Decimal = field(default=0)
    write_capacity_units: Decimal = field(default=0)
    backup: str = field(default=Backup.ENABLED.value)
    auto_backup_status: str = field(default=AutoBackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    backup_schedule: str | None = field(default='0 0 * * *')
    indexes: List[IndexInfo] = field(default_factory=list)


@dataclass
class UpdateTableRequest:
    description: str

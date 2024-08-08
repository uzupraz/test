from dataclasses import dataclass, field
from typing import List
from decimal import Decimal

from enums import TableStatus, IndexStatus, AutoBackupStatus, Backup, BackupStatus, BackupType

@dataclass
class ListTableResponse:
    name: str
    id: str
    size: float


@dataclass
class IndexInfo:
    name: str
    partition_key: str
    sort_key: str | None = field(default=None)
    status: str = field(default=IndexStatus.ACTIVE.value)
    size: float = field(default=0)
    item_count: int = field(default=0)


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
    total_indexes: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backup: str = field(default=Backup.ENABLED.value)
    auto_backup_status: str = field(default=AutoBackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    backup_schedule: str | None = field(default='0 0 * * *')
    table_arn: str | None = field(default=None)
    indexes: List[IndexInfo] = field(default_factory=list)


@dataclass
class UpdateTableRequest:
    description: str


@dataclass
class BackupDetail:
    id: str
    name: str | None = field(default=None)
    status: str = field(default=BackupStatus.ACTIVE.value)
    creation_time: str | None = field(default=None)
    type: str = field(default=BackupType.AUTO.value)
    size: int = field(default=0)

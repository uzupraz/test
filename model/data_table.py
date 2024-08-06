from dataclasses import dataclass, field
from typing import List

from enums import TableStatus, IndexStatus, AutoBackupStatus, Backup

@dataclass
class ListTableResponse:
    name: str
    id: str
    size: float


@dataclass
class IndexInfo:
    name: str
    partition_key: str
    sort_key: str
    status: str = field(default=IndexStatus.ACTIVE.value)
    item_count: int = field(default=0)


@dataclass
class CustomerTableInfo:
    owner_id: str
    table_id: str
    table_name: str
    original_table_name: str
    partition_key: str
    sort_key: str
    description: str | None = field(default=None)
    created_by: str | None = field(default=None)
    creation_time: str | None = field(default=None)
    total_indices: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backup: str = field(default=Backup.ENABLED.value)
    auto_backup_status: str = field(default=AutoBackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    next_backup_schedule_cron_pattern: str | None = field(default='0 0 * * *')
    last_backup_schedule_cron_pattern: str | None = field(default='0 0 * * *')
    indices: List[IndexInfo] = field(default_factory=list)


@dataclass
class UpdateTableRequest:
    description: str


@dataclass
class IndexDetails:
    name: str
    partition_key: str
    sort_key: str
    status: str = field(default=IndexStatus.ACTIVE.value)
    size: int = field(default=0)
    item_count: int = field(default=0)


@dataclass
class TableDetails:
    owner_id: str
    table_id: str
    table_name: str
    partition_key: str
    sort_key: str
    description: str | None = field(default=None)
    created_by: str | None = field(default=None)
    creation_time: str | None = field(default=None)
    total_indices: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backup: str = field(default=Backup.ENABLED.value)
    auto_backup_status: str = field(default=AutoBackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    next_backup_schedule_cron_pattern: str | None = field(default='0 0 * * *')
    last_backup_schedule_cron_pattern: str | None = field(default='0 0 * * *')
    indices: List[IndexDetails] = field(default_factory=list)


@dataclass
class DynamoDBTableDetails:
    size: int

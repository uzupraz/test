from dataclasses import dataclass, field
from typing import List

from enums import BackupStatus, TableStatus, AlarmStatus, IndexStatus

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
    size: int = field(default=0)
    item_count: int = field(default=0)


@dataclass
class CustomerTableInfo:
    owner_id: str
    table_id: str
    table_name: str
    original_table_name: str
    description: str | None = field(default=None)
    created_by: str | None = field(default=None)
    creation_time: str | None = field(default=None)
    total_indexes: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backups: str = field(default=BackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    alarms: str = field(default=AlarmStatus.OK.value)
    next_backup_schedule: str | None = field(default=None)
    last_backup_schedule: str | None = field(default=None)
    indices: List[IndexInfo] = field(default_factory=list)


@dataclass
class UpdateTableRequest:
    description: str


@dataclass
class UpdateTableResponse:
    table_id: str
    table_name: str
    description: str | None = field(default=None)
    created_by: str | None = field(default=None)
    creation_time: str | None = field(default=None)
    total_indexes: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backups: str = field(default=BackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    alarms: str = field(default=AlarmStatus.OK.value)
    next_backup_schedule: str | None = field(default=None)
    last_backup_schedule: str | None = field(default=None)


@dataclass
class TableDetailsResponse:
    table_id: str
    table_name: str
    description: str | None = field(default=None)
    created_by: str | None = field(default=None)
    creation_time: str | None = field(default=None)
    total_indexes: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backups: str = field(default=BackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    alarms: str = field(default=AlarmStatus.OK.value)
    next_backup_schedule: str | None = field(default=None)
    last_backup_schedule: str | None = field(default=None)
    indices: List[IndexInfo] = field(default_factory=list)

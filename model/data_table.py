from dataclasses import dataclass, field

from enums import BackupStatus, TableStatus, AlarmStatus

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
class CustomerTableContentPagination:
    size: int
    last_evaluated_key: str | None

@dataclass
class CustomerTableContent:
    items: list[any]
    pagination: CustomerTableContentPagination
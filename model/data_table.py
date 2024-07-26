from dataclasses import dataclass, field

from enums import BackupStatus, TableStatus, AlarmStatus
from datetime import datetime, timezone

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
    total_indexes: int = field(default=0)
    read_capacity_units: int = field(default=0)
    write_capacity_units: int = field(default=0)
    backups: str = field(default=BackupStatus.ENABLED.value)
    table_status: str = field(default=TableStatus.ACTIVE.value)
    alarms: str = field(default=AlarmStatus.OK.value)
    next_backup_schedule: str = field(default=None)
    last_backup_schedule: str = field(default=None)


@dataclass
class UpdateTableRequest:
    description: str


@dataclass
class UpdateTableResponse:
    table_id: str
    table_name: str
    description: str
    created_by: str
    creation_time: str
    total_indexes: int
    read_capacity_units: int
    write_capacity_units: int
    backups: str
    table_status: str
    alarms: str
    next_backup_schedule: str
    last_backup_schedule: str


    @classmethod
    def from_customer_table_info(cls, customer_table_info:CustomerTableInfo) -> 'UpdateTableResponse':
        """
        Create an UpdateTableResponse instance from a CustomerTableInfo instance.

        Args:
            customer_table_info (CustomerTableInfo): The CustomerTableInfo instance to convert.

        Returns:
            UpdateTableResponse: The created UpdateTableResponse instance.
        """
        update_table_response = UpdateTableResponse(
        table_id=customer_table_info.table_id,
        table_name=customer_table_info.table_name,
        description=customer_table_info.description,
        created_by=customer_table_info.created_by,
        creation_time=customer_table_info.creation_time,
        total_indexes=customer_table_info.total_indexes,
        read_capacity_units=customer_table_info.read_capacity_units,
        write_capacity_units=customer_table_info.write_capacity_units,
        backups=customer_table_info.backups,
        table_status=customer_table_info.table_status,
        alarms=customer_table_info.alarms,
        next_backup_schedule=customer_table_info.next_backup_schedule,
        last_backup_schedule=customer_table_info.last_backup_schedule
        )
        return update_table_response

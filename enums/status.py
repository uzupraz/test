from enum import Enum


class ServiceStatus(Enum):
    SUCCESS='SUCCESS'
    FAILURE='FAILURE'


class APIStatus(Enum):
    START='START'
    SUCCESS='SUCCESS'
    FAILURE='FAILURE'


class SystemStatus(Enum):
    ONLINE='ONLINE'


class BackupType(Enum):
    AUTO='AUTO'
    MANUAL='MANUAL'


class Backup(Enum):
    ENABLED='ON'
    DISABLED='OFF'


class AutoBackupStatus(Enum):
    ENABLED='ENABLED'
    DISABLED='DISABLED'


class BackupStatus(Enum):
    ACTIVE='ACTIVE'


class TableStatus(Enum):
    CREATING='CREATING'
    UPDATING='UPDATING'
    DELETING='DELETING'
    ACTIVE='ACTIVE'
    INACCESSIBLE_ENCRYPTION_CREDENTIALS='INACCESSIBLE_ENCRYPTION_CREDENTIALS'
    ARCHIVING='ARCHIVING'
    ARCHIVED='ARCHIVED'


class IndexStatus(Enum):
    CREATING='CREATING'
    UPDATING='UPDATING'
    DELETING='DELETING'
    ACTIVE='ACTIVE'

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
    USER='USER'
    SYSTEM='SYSTEM'
    AWS_BACKUP='AWS_BACKUP'


class Backup(Enum):
    ENABLED='ON'
    DISABLED='OFF'


class AutoBackupStatus(Enum):
    ENABLED='ENABLED'
    DISABLED='DISABLED'


class BackupStatus(Enum):
    CREATING='CREATING'
    ACTIVE='ACTIVE'
    DELETED='DELETED'


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

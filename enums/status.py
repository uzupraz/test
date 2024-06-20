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

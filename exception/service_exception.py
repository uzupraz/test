from dataclasses import dataclass
from enums import ServiceStatus


@dataclass
class ServiceException(Exception):

    status_code: int
    status: ServiceStatus
    message: str
    cause: Exception = None

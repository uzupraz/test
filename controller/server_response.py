from dataclasses import dataclass, asdict
from enums import ServiceStatus
import datetime


@dataclass
class ServerResponse:

    code: str
    message: str
    payload: any = None
    timestamp: str = datetime.datetime.now().isoformat()


    @classmethod
    def success(cls, payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(ServiceStatus.SUCCESS.value, 'Successfully performed operation.', payload=payload)


    @classmethod
    def created(cls, payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(ServiceStatus.SUCCESS.value, 'Successfully created resource.', payload=payload)


    @classmethod
    def response(cls, code:ServiceStatus, message:str,  payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(code.value, message, payload=payload)


    @classmethod
    def error(cls, code:ServiceStatus, message='Could not perform operation', payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(code.value, message, payload=payload)

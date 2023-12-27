from dataclasses import dataclass, asdict
import datetime

from enums import ServiceStatus
from context import RequestContext


@dataclass
class ServerResponse:

    code: str
    message: str
    payload: any = None
    request_id: str = None
    timestamp: str = datetime.datetime.now().isoformat()


    @classmethod
    def success(cls, payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(ServiceStatus.SUCCESS.value, 'Successfully performed operation.', payload=payload, request_id=RequestContext.get_request_id())


    @classmethod
    def created(cls, payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(ServiceStatus.SUCCESS.value, 'Successfully created resource.', payload=payload, request_id=RequestContext.get_request_id())


    @classmethod
    def response(cls, code:ServiceStatus, message:str,  payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(code.value, message, payload=payload, request_id=RequestContext.get_request_id())


    @classmethod
    def error(cls, code:ServiceStatus, message='Could not perform operation', payload=None) -> 'ServerResponse':
        payload = asdict(payload) if payload else payload
        return ServerResponse(code.value, message, payload=payload, request_id=RequestContext.get_request_id())

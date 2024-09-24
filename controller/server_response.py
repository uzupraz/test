from dataclasses import dataclass, asdict, is_dataclass
import datetime

from enums import ServiceStatus
from context import RequestContext
from utils import DataTypeUtils


@dataclass
class ServerResponse:

    code: str
    message: str
    payload: any = None
    request_id: str = None
    timestamp: str = datetime.datetime.now().isoformat()

    @classmethod
    def get_payload_as_dict(cls, payload) -> dict:
        """
        Convert the payload into dictionary if required and also sanitizes fields that json library cannot process. Eg. Decimal.
        Args:
            payload(any): Any dataclass type or list of dataclass types
        Returns:
            An equivalent dictionary with data types sanitized
        """

        if isinstance(payload, list):
            result = []
            for item in payload:
                result.append(ServerResponse.get_payload_as_dict(item))
            result = DataTypeUtils.convert_decimals_to_float_or_int(result)
            return result
        elif isinstance(payload, dict):
            payload = DataTypeUtils.convert_decimals_to_float_or_int(payload)
            return payload
        elif is_dataclass(payload):
            result = asdict(payload)
            result = DataTypeUtils.convert_decimals_to_float_or_int(result)
            return result

        raise ValueError('Unsupported response body type')


    @classmethod
    def success(cls, payload=None) -> 'ServerResponse':
        payload = ServerResponse.get_payload_as_dict(payload) if payload else payload
        return ServerResponse(ServiceStatus.SUCCESS.value, 'Successfully performed operation.', payload=payload, request_id=RequestContext.get_request_id())


    @classmethod
    def created(cls, payload=None) -> 'ServerResponse':
        payload = ServerResponse.get_payload_as_dict(payload) if payload else payload
        return ServerResponse(ServiceStatus.SUCCESS.value, 'Successfully created resource.', payload=payload, request_id=RequestContext.get_request_id())


    @classmethod
    def response(cls, code:ServiceStatus, message:str,  payload=None) -> 'ServerResponse':
        payload = ServerResponse.get_payload_as_dict(payload) if payload else payload
        return ServerResponse(code.value, message, payload=payload, request_id=RequestContext.get_request_id())


    @classmethod
    def error(cls, code:ServiceStatus, message='Could not perform operation', payload=None) -> 'ServerResponse':
        payload = ServerResponse.get_payload_as_dict(payload) if payload else payload
        return ServerResponse(code.value, message, payload=payload, request_id=RequestContext.get_request_id())

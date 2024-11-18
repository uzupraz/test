from dataclasses import dataclass
from typing import Generator
from flask import Response, stream_with_context
import datetime

from enums import ServiceStatus
from context import RequestContext


@dataclass
class ServerStreamResponse:
    """
    A class to handle streaming responses in a standardized way across the application.
    Similar to ServerResponse but specifically designed for streaming data.
    """
    code: str
    message: str
    stream_generator: Generator
    content_type: str = 'text/plain'
    request_id: str = None
    timestamp: str = datetime.datetime.now().isoformat()


    def __post_init__(self):
        self.default_headers = {
            'Cache-Control': 'no-cache',
            'Transfer-Encoding': 'chunked',
            'X-Accel-Buffering': 'no',
            'Access-Control-Allow-Origin': '*',
        }


    def create_response(self) -> Response:
        """
        Creates a Flask Response object with the streaming content and appropriate headers
        """
        headers = self.default_headers.copy()
        headers['Content-Type'] = self.content_type
        headers['X-Request-ID'] = self.request_id

        def response_generator():
            # Send initial metadata if needed
            if self.code != ServiceStatus.SUCCESS.value:
                yield f"Error: {self.message}\n"
            
            # Stream the actual content
            try:
                for chunk in self.stream_generator:
                    yield chunk
            except Exception as e:
                yield f"Stream Error: {str(e)}\n"

        return Response(
            stream_with_context(response_generator()),
            headers=headers
        )


    @classmethod
    def success(cls, stream_generator: Generator, content_type: str = 'text/plain') -> Response:
        """
        Creates a successful streaming response
        """
        response = cls(
            code=ServiceStatus.SUCCESS.value,
            message='Successfully initiated stream.',
            stream_generator=stream_generator,
            content_type=content_type,
            request_id=RequestContext.get_request_id()
        )
        return response.create_response()


    @classmethod
    def error(cls, 
              code: ServiceStatus, 
              message: str = 'Could not perform streaming operation', 
              stream_generator: Generator = None) -> Response:
        """
        Creates an error streaming response
        """
        if stream_generator is None:
            stream_generator = (x for x in [])  
            
        response = cls(
            code=code.value,
            message=message,
            stream_generator=stream_generator,
            request_id=RequestContext.get_request_id()
        )
        return response.create_response()


    @classmethod
    def response(cls, 
                code: ServiceStatus, 
                message: str, 
                stream_generator: Generator,
                content_type: str = 'text/plain') -> Response:
        """
        Creates a custom streaming response with specified code and message
        """
        response = cls(
            code=code.value,
            message=message,
            stream_generator=stream_generator,
            content_type=content_type,
            request_id=RequestContext.get_request_id()
        )
        return response.create_response()
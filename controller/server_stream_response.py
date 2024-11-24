import json
from dataclasses import dataclass
from typing import Generator
from flask import Response, stream_with_context
from enums import ServiceStatus
from .server_response import ServerResponse


@dataclass
class ServerStreamResponse:
    """
    A class to handle streaming responses in a standardized way across the application.
    It is designed to handle streaming data and to generate responses with appropriate headers
    for streaming, such as 'Transfer-Encoding' and 'Cache-Control'.
    """
    stream_generator: Generator  
    content_type: str = 'text/plain' 
    code: str = None  

    def __post_init__(self):
        """
        Initializes default headers to control caching and streaming.
        """
        self.default_headers = {
            'Cache-Control': 'no-cache',  # Prevents caching of the response.
            'Transfer-Encoding': 'chunked',  # Specifies that the response will be streamed in chunks.
            'X-Accel-Buffering': 'no',  # Disables buffering to ensure immediate delivery of data.
            'Access-Control-Allow-Origin': '*',  # Allows cross-origin requests (CORS).
        }

    def create_response(self) -> Response:
        """
        Creates a Flask Response object that streams the content via the provided generator.

        The response includes appropriate headers for streaming and handles exceptions
        by yielding an error message if something goes wrong during streaming.

        Returns:
            Response: A Flask Response object containing the streaming data or error message.
        """
        headers = self.default_headers.copy()
        headers['Content-Type'] = self.content_type

        def response_generator():
            """
            A generator that yields chunks of data from the stream_generator, 
            or an error response in case of an exception.
            """
            try:
                for chunk in self.stream_generator:
                    yield chunk  # Yield each chunk from the stream generator.
            except Exception:
                error_response = ServerResponse.error(
                    code=ServiceStatus.FAILURE,
                    message='Could not generate stream response'
                )
                yield json.dumps(error_response.__dict__)  

        return Response(
            stream_with_context(response_generator()),  
            headers=headers,  
        )

    @classmethod
    def success(cls, stream_generator: Generator) -> Response:
        """
        Creates a successful streaming response using the provided stream generator.

        Args:
            stream_generator (Generator): The generator that yields data chunks to be streamed.

        Returns:
            Response: A Flask Response object containing the streamed data.
        """
        response = cls(
            stream_generator=stream_generator,
        )
        return response.create_response()
import json

from dataclasses import dataclass
from typing import Generator
from flask import Response, stream_with_context, make_response
from enums import ServiceStatus
from .server_response import ServerResponse
from controller import common_controller as common_ctrl

log = common_ctrl.log

@dataclass
class ServerStreamResponse:
    """
    A class to handle streaming responses with robust error handling.
    """
    stream_generator: Generator
    content_type: str = 'text/plain'


    def create_response(self) -> Response:
        """
        Creates a Flask Response object.

        Returns:
            Response: A Flask Response object.
        """
        try:
            iter_gen = iter(self.stream_generator)
            first_item = next(iter_gen, None)

            if first_item is None:
                return self._create_error_response()

            def safe_stream_generator():
                """
                Wrapped generator with error handling.
                """
                try:
                    yield first_item
                    yield from iter_gen
                except Exception as e:
                    log.exception("Failed to create streaming response: %s", e)
                    error_response = self._create_error_response()
                    yield error_response.get_data(as_text=True)

            return Response(
                stream_with_context(safe_stream_generator()),
                status=200,
                headers={
                    "Cache-Control": "no-cache",
                    "Transfer-Encoding": "chunked",
                    "X-Accel-Buffering": "no",
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": self.content_type,
                },
            )
        except Exception as e:
            log.exception("Failed to create streaming response: %s", e)
            return self._create_error_response()


    @classmethod
    def generate(cls, stream_generator: Generator, content_type: str = "text/plain") -> Response:
        """
        Creates a streaming response.

        Args:
            stream_generator (Generator): The generator that yields data chunks to be streamed.
            content_type (str): Content-Type header value for the response.

        Returns:
            Response: A Flask Response object containing the streamed data.
        """
        return cls(stream_generator=stream_generator, content_type=content_type).create_response()
    

    def _create_error_response(self) -> Response:
        """
        Creates an error response.
        """
        error_response = ServerResponse.error(
            code=ServiceStatus.FAILURE,
            message="Could not generate stream response",
        )
        return make_response(json.dumps(error_response.__dict__), 400)

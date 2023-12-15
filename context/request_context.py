import uuid
from flask import g, has_request_context

class RequestContext:
    """
    Class to manage request-related variables like request_id using Flask's application context (g).
    """

    @classmethod
    def update_request_id(cls, lambda_request_id:str=None) -> None:
        """
        Update the request ID based on Lambda request ID or generate a new UUID.

        Args:
            lambda_request_id (str): Unique lambda request id for each request.
        """
        if lambda_request_id:
            # Update request ID using Lambda request ID
            g.request_id = lambda_request_id
        else:
            # Update request ID generating a new UUID
            g.request_id = str(uuid.uuid4())


    @classmethod
    def get_request_id(cls) -> str:
        """
        Get the current request ID from the Flask application context (g).

        Returns:
            str: Current request ID.
        """
        if has_request_context():
            if 'request_id' not in g:
                g.request_id = str(uuid.uuid4())
            return g.request_id
        return '-'

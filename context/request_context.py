import uuid
from flask import g, has_request_context

from model import User

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
   
    
    @classmethod
    def store_authenticated_user(cls, event: dict) -> None:
        """
        Store the user object in the Flask application context (g).

        Args:
            event: The event object containing the token validation info.
        """
        claims = event['requestContext']['authorizer']['claims']
        user = User.from_authorizer_claims(claims)
        g.user = user

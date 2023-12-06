# Since flask restx does not work with flask > 3.0.0
# See link below for current open issue ticket for flask_restx
# https://github.com/python-restx/flask-restx/issues/566
import restx_monkey as monkey
monkey.patch_restx()

from flask_restx import Api
from dataclasses import asdict

from .common_controller import health_api as health_ns
from .workflow_resource import api as workflow_ns
from .server_response import ServerResponse
from enums import ServiceStatus

api = Api(version='1.0', title='InterconnectHub Management API', description='InterconnectHub Management for Workflow related services.')
namespaces = [health_ns, workflow_ns]

for ns in namespaces:
    api.add_namespace(ns)


@api.errorhandler(Exception)
def handle_all_exceptions(e:Exception):
    """
    This function handles all exceptions that occur within the API.

    Args:
        e (Exception): The exception to be handled

    Returns:
        A tuple with response and a status code
    """
    return asdict(ServerResponse.error(ServiceStatus.FAILURE)), 500

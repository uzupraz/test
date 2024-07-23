# Since flask restx does not work with flask > 3.0.0
# See link below for current open issue ticket for flask_restx
# https://github.com/python-restx/flask-restx/issues/566
import restx_monkey as monkey
# This order is required as in the idm project. Changing the order could lead to unexpected behavior or errors.
monkey.patch_restx()

from flask_restx import Api
from flask import request
from dataclasses import asdict
from werkzeug.exceptions import HTTPException

from .common_controller import log, api as health_ns
from .workflow_resource import api as workflow_ns
from .dashboard_resource import api as dashboard_ns
from .processor_template_resource import api as processors_ns
from .data_studio_resource import api as data_studio_ns
from .files_resource import api as files_ns
from .data_table_resource import api as data_table_ns
from .server_response import ServerResponse
from enums import ServiceStatus, APIStatus
from exception import ServiceException


api = Api(version='1.0', title='InterconnectHub Management API', description='InterconnectHub Management for Workflow related services.', doc='/api-docs')
namespaces = [health_ns, workflow_ns, processors_ns, files_ns, dashboard_ns, data_studio_ns, data_table_ns]

for ns in namespaces:
    api.add_namespace(ns)


@api.errorhandler(HTTPException)
def handle_bad_request(e):
    # Use getattr to safely get the data attribute
    log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE)
    error_data = getattr(e, 'data', None)
    return asdict(ServerResponse.error(ServiceStatus.FAILURE, message=e.description, payload=error_data)), e.code


@api.errorhandler(ServiceException)
def handle_service_exception(e):
    log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE)
    return asdict(ServerResponse.error(e.status, message=e.message)), e.status_code


@api.errorhandler(Exception)
def handle_all_exceptions(e:Exception):
    """
    This function handles all exceptions that occur within the API.

    Args:
        e (Exception): The exception to be handled

    Returns:
        A tuple with response and a status code
    """
    log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE)
    return asdict(ServerResponse.error(ServiceStatus.FAILURE)), 500

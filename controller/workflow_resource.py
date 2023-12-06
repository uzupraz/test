from flask_restx import  fields, Resource, Namespace

from .server_response import ServerResponse
from .common_controller import workflow_dto

api = Namespace('Workflow API', description='Manages workflow related operations.', path='/workflow')
log = api.logger


@api.route('/')
class WorkflowResource(Resource):


    @api.expect(workflow_dto, validate=True)
    def post(self):
        """
        Create a workflow
        """
        return ServerResponse.success(), 200

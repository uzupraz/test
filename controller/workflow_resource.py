from flask_restx import Resource, Namespace
from dacite import from_dict

from .server_response import ServerResponse
from .common_controller import workflow_dto, server_response
from configuration import AWSConfig
from repository import WorkflowRepository
from service import WorkflowService
from model import Workflow


api = Namespace('Workflow API', description='Manages workflow related operations.', path='/workflow')
log = api.logger


@api.route('/')
class WorkflowResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.config = AWSConfig.get_instance()
        self.workflow_repository = WorkflowRepository.get_instance(self.config)
        self.workflow_service = WorkflowService(self.workflow_repository)


    @api.expect(workflow_dto, validate=True)
    @api.marshal_with(server_response, skip_none=True)
    def post(self):
        """
        Create a workflow
        """
        workflow_request_dto = api.payload
        workflow = from_dict(data_class=Workflow, data=workflow_request_dto)
        self.workflow_service.save_workflow(workflow)
        return ServerResponse.created(), 200

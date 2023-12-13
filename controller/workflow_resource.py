from flask_restx import fields, Resource, Namespace
from dacite import from_dict
from flask import request

from .server_response import ServerResponse
from .common_controller import workflow_dto, server_response
from configuration import AWSConfig, AppConfig
from repository import WorkflowRepository
from service import WorkflowService
from model import Workflow
from enums import APIStatus


api = Namespace('Workflow API', description='Manages workflow related operations.', path='/workflow')
log = api.logger

create_workflow_response_dto = api.inherit('Create Workflow Response', server_response, {
       'payload': fields.Nested(workflow_dto)
})


@api.route('/')
class WorkflowResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig.get_instance()
        self.app_config = AppConfig.get_instance()
        self.workflow_repository = WorkflowRepository.get_instance(self.app_config, self.aws_config)
        self.workflow_service = WorkflowService.get_instance(self.workflow_repository)


    @api.expect(workflow_dto, validate=True)
    @api.marshal_with(create_workflow_response_dto, skip_none=True)
    def post(self):
        """
        Create a workflow
        """
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        workflow_request_dto = api.payload
        workflow = from_dict(data_class=Workflow, data=workflow_request_dto)
        created_workflow = self.workflow_service.save_workflow(workflow)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.created(payload=created_workflow), 200

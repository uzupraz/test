
from flask_restx import fields, Resource, Namespace,reqparse
from flask import request

from .server_response import ServerResponse
from .common_controller import server_response
from configuration import AWSConfig, AppConfig
from repository import WorkflowRepository
from service import WorkflowService
from enums import APIStatus

api = Namespace(name='Workflow Execution Events API', description='Returns workflow execution events.', path='/interconnecthub/workflow/execution-events')
log = api.logger

parser = reqparse.RequestParser()
parser.add_argument("start_date", help="Start date for the workflow executions.", required=True)
parser.add_argument("end_date", help="End date for the workflow executions.", required=True)

get_workflow_execution_events_response_dto = api.inherit('Get Workflow Execution Events Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Execution Events',{
        "date": fields.String(description='Date of the event'),
        "failed_events": fields.Integer(description='Number of failed events'),
        "fluent_executions": fields.Integer(description='Number of fluent executions'),
    })))
})

@api.route("/")
class WorkflowExecutionEventsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.workflow_repository = WorkflowRepository.get_instance(self.app_config, self.aws_config)
        self.workflow_service = WorkflowService.get_instance(self.workflow_repository)
    

    @api.expect(parser, validate=True)
    @api.marshal_with(get_workflow_execution_events_response_dto, skip_none=True)
    def get(self):
        """
        Get workflow execution events
        """
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date: str = request.args.get('start_date')
        end_date: str = request.args.get('end_date')
        owner_id: str = request.args.get("owner")
        workflow_execution_events = self.workflow_service.get_workflow_execution_events(owner_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=workflow_execution_events), 200
    
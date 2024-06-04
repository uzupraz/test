from flask_restx import fields, Resource, Namespace,reqparse
from flask import request

from .server_response import ServerResponse
from .common_controller import server_response
from configuration import AWSConfig, AppConfig
from repository import WorkflowRepository
from service import WorkflowService
from enums import APIStatus

api = Namespace(name='Workflow Stats API', description='Returns the stats about the workflows like active workflows, failed events, fluent executions and system status.', path='/interconnecthub/workflow/stats')
log = api.logger

parser = reqparse.RequestParser()
parser.add_argument("start_date", help="Start date for the stats.", required=True)
parser.add_argument("end_date", help="End date for the stats.", required=True)

get_workflow_stats_response_dto = api.inherit('Get Worflow Stats Response',server_response, {
    'payload': fields.Nested(api.model('Workflow Stats', {
        'active_workflows': fields.Integer(description='Number of active workflows'),
        'failed_events': fields.Integer(description='Number of failed events'),
        'fluent_executions': fields.Integer(description='Number of fluent executions'),
        'system_status': fields.String(description='System status')
    }))
})

@api.route("/")
class WorkflowStatsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.workflow_repository = WorkflowRepository.get_instance(self.app_config, self.aws_config)
        self.workflow_service = WorkflowService.get_instance(self.workflow_repository)
    

    @api.expect(parser, validate=True)
    @api.marshal_with(get_workflow_stats_response_dto, skip_none=True)
    def get(self):
        """
        Get the stats about the workflows
        """
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date: str = request.args.get('start_date')
        end_date: str = request.args.get('end_date')
        workflow_stats = self.workflow_service.get_workflow_stats(start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.ok(payload=workflow_stats), 200
    
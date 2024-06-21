from flask_restx import Namespace, Resource, fields, reqparse
from flask import g, request

from configuration import AWSConfig, AppConfig, OpensearchConfig
from .server_response import ServerResponse
from .common_controller import server_response
from enums import APIStatus
from repository import WorkflowRepository
from service import DashboardService, OpensearchService
from model import User


api = Namespace("Dashboard API", description="API for the dashboard home", path="/interconnecthub/dashboard")
log = api.logger


app_config = AppConfig()
aws_config = AWSConfig()
opensearch_config = OpensearchConfig()

workflow_repository = WorkflowRepository(app_config, aws_config)
opensearch_service = OpensearchService(config=opensearch_config)
dashboard_service = DashboardService(workflow_repository=workflow_repository, opensearch_service=opensearch_service)


parser = reqparse.RequestParser()
parser.add_argument("start_date", help="Start date for the stats. e.g. YYYY-MM-DDTHH:MM:SS.sssZ", required=True)
parser.add_argument("end_date", help="End date for the stats. e.g YYYY-MM-DDTHH:MM:SS.sssZ", required=True)


workflow_stats_response_dto = api.inherit('Get Workflow Stats Response',server_response, {
    'payload': fields.Nested(api.model('Workflow Stats', {
        'active_workflows_count': fields.Integer(description='Total number of active workflows'),
        'failed_executions_count': fields.Integer(description='Total number of failed executions'),
        'total_executions_count': fields.Integer(description='Total number of executions'),
        'system_status': fields.String(description='Status of the current system')
    }))
})


workflow_integrations_response_dto = api.inherit('Get Workflow Integrations Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Integrations', {
        "failed_executions_count": fields.Integer(description='Total number of failed executions of an workflow'),
        "failed_executions_ratio": fields.Float(description='Failure ratio of an workflow'),
        "last_event_date": fields.String(description='Last event date of the workflow'),
        "workflow": fields.Nested(api.model("Workflow", {
            "id": fields.String(description='Workflow ID'),
            "name": fields.String(description='Workflow name'),
        }))
    })))
})


workflow_failures_response_dto = api.inherit('Get Workflow Failures Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Failures',{
        "color": fields.String(description='Color of the workflow for visualization'),
        "workflow_name": fields.String(description='Workflow Name'),
        "failures": fields.List(fields.Nested(api.model("Failure", {
            "error_code": fields.String(description='Error code'),
            "failure_ratio": fields.Float(description='Failure ratio'),
            "severity": fields.Float(description='Severity of the failure.'),
        })))
    })))
})


workflow_failed_events_response_dto = api.inherit('Get Workflow Failed Events Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Failed Events',{
        "date": fields.String(description='Date of the event'),
        "error_code": fields.String(description='Error code'),
        "event_id": fields.String(description='Event ID'),
        "workflow": fields.Nested(api.model("Workflow", {
            "id": fields.String(description='Workflow ID'),
            "name": fields.String(description='Workflow Name'),
        }))
    })))
})


workflow_execution_metrics_response_dto = api.inherit('Get workflow execution metrics',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Execution Events',{
        "date": fields.String(description='Date of the execution'),
        "failed_executions": fields.Integer(description='Total number of failed executions'),
        "total_executions": fields.Integer(description='Total number of executions'),
    })))
})


@api.route("/stats")
class WorkflowStatsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get the stats about the workflows.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_stats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user_data = g.get("user")
        user = User(**user_data)
        workflow_stats = dashboard_service.get_workflow_stats(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_stats), 200


@api.route("/integrations")
class WorkflowIntegrationsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get all the active workflow integrations.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_integrations_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        workflow_integrations = dashboard_service.get_workflow_integrations(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_integrations), 200


@api.route("/failures")
class WorkflowFailuresResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get workflow failures.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_failures_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        workflow_failures = dashboard_service.get_workflow_failures(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_failures), 200


@api.route("/failed-events")
class WorkflowFailedEventsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get workflow failed events.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_failed_events_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        workflow_failed_events = dashboard_service.get_workflow_failed_events(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_failed_events), 200


@api.route("/executions")
class WorkflowExecutionEventsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get workflow execution events.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_execution_metrics_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user_data = g.get("user")
        user = User(**user_data)
        workflow_execution_metrics = dashboard_service.get_workflow_execution_metrics_by_date(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_execution_metrics), 200

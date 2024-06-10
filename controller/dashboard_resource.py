from flask_restx import Namespace, Resource, fields, reqparse
from flask import g, request

from configuration import AWSConfig, AppConfig
from .server_response import ServerResponse
from .common_controller import server_response
from enums import APIStatus
from repository import WorkflowRepository
from service import WorkflowService


api = Namespace("Dashboard API", description="API for the dashboard home", path="/interconnecthub/dashboard")
log = api.logger


app_config = AppConfig()
aws_config = AWSConfig()
workflow_repository = WorkflowRepository.get_instance(app_config, aws_config)
workflow_service = WorkflowService.get_instance(workflow_repository)


parser = reqparse.RequestParser()
parser.add_argument("start_date", help="Start date for the stats. e.g. YYYY-MM-DD", required=True)
parser.add_argument("end_date", help="End date for the stats. e.g YYYY-MM-DD", required=True)


workflow_stats_response_dto = api.inherit('Get Workflow Stats Response',server_response, {
    'payload': fields.Nested(api.model('Workflow Stats', {
        'active_workflows': fields.Integer(description='Number of active workflows'),
        'failed_events': fields.Integer(description='Number of failed events'),
        'fluent_executions': fields.Integer(description='Number of fluent executions'),
        'system_status': fields.String(description='System status')
    }))
})


workflow_integrations_response_dto = api.inherit('Get Workflow Integrations Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Integrations', {
        "failure_count": fields.Integer(description='Number of failed events'),
        "failure_ratio": fields.Float(description='Failure ratio'),
        "last_event_date": fields.String(description='Last event date'),
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


workflow_execution_events_response_dto = api.inherit('Get Workflow Execution Events Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Execution Events',{
        "date": fields.String(description='Date of the event'),
        "failed_events": fields.Integer(description='Number of failed events'),
        "fluent_executions": fields.Integer(description='Number of fluent executions'),
    })))
})


@api.route("/stats")
class WorkflowStatsResource(Resource):
    
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

        
    @api.doc(description="Get the stats about the workflows")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_stats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        owner_id = user.sub
        workflow_stats = workflow_service.get_workflow_stats(owner_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=workflow_stats), 200


@api.route("/integrations")
class WorkflowIntegrationsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    
    @api.doc(description="Get all the active workflow integrations.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_integrations_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        owner_id = user.sub
        workflow_integrations = workflow_service.get_workflow_integrations(owner_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=workflow_integrations), 200
    

@api.route("/failures")
class WorkflowFailuresResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get workflow failures.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_failures_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        owner_id = user.sub
        workflow_failures = workflow_service.get_workflow_failures(owner_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=workflow_failures), 200
    

@api.route("/failed-events")
class WorkflowFailedEventsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
    

    @api.doc(description="Get workflow failed events.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_failed_events_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        owner_id = user.sub
        workflow_failed_events = workflow_service.get_workflow_failed_events(owner_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=workflow_failed_events), 200


@api.route("/execution-events")
class WorkflowExecutionEventsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
    

    @api.doc(description="Get workflow execution events.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_execution_events_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        user = g.get("user")
        owner_id = user.sub
        workflow_execution_events = workflow_service.get_workflow_execution_events(owner_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=workflow_execution_events), 200
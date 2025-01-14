from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask_restx import Namespace, Resource, fields, reqparse
from flask import g, request

from configuration import AWSConfig, AppConfig, OpensearchConfig, PostgresConfig
from ..server_response import ServerResponse
from ..common_controller import server_response
from enums import APIStatus
from repository import WorkflowRepository, ExecutionSummaryRepository
from service.v2 import DashboardService
from model import User
from exception import ServiceException
from enums import ServiceStatus


api = Namespace("Dashboard API V2", description="API for the dashboard home", path="/interconnecthub/v2/dashboard/")
log = api.logger


app_config = AppConfig()
aws_config = AWSConfig()
postgres_config = PostgresConfig()
execution_summary_repository = ExecutionSummaryRepository(postgres_config=postgres_config)
workflow_repository = WorkflowRepository(app_config, aws_config)
dashboard_service = DashboardService(workflow_repository=workflow_repository, execution_summary_repository=execution_summary_repository)


current_date = datetime.now()
three_months_ago = current_date - relativedelta(months=3)


parser = reqparse.RequestParser()
parser.add_argument("start_date", help="Start date for the stats. e.g. YYYY-MM-DD HH:MM:SS.mmmmmm", required=False, default=three_months_ago.isoformat())
parser.add_argument("end_date", help="End date for the stats. e.g YYYY-MM-DD HH:MM:SS.mmmmmm", required=False, default=current_date.isoformat())


workflow_stats_response_dto = api.inherit('Get Workflow Stats Response',server_response, {
    'payload': fields.Nested(api.model('Workflow Stats', {
        'active_workflows_count': fields.Integer(description='Total number of active workflows'),
        'failed_executions_count': fields.Integer(description='Total number of failed executions'),
        'total_executions_count': fields.Integer(description='Total number of executions'),
        'system_status': fields.String(description='Status of the current system')
    }))
})


workflow_execution_metrics_response_dto = api.inherit('Get workflow execution metrics',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Execution Events',{
        "date": fields.String(description='Date of the execution'),
        "failed_executions": fields.Integer(description='Total number of failed executions'),
        "total_executions": fields.Integer(description='Total number of executions'),
    })))
})


workflow_integrations_response_dto = api.inherit('Get Workflow Integrations Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Integrations', {
        "failed_executions_count": fields.Integer(description='Total number of failed executions of an workflow'),
        "total_executions_count": fields.Integer(description='Total number of executions of an workflow'),
        "failed_executions_ratio": fields.Float(description='Failure ratio of an workflow'),
        "last_event_date": fields.String(description='Last event date of the workflow'),
        "workflow": fields.Nested(api.model("Workflow", {
            "id": fields.String(description='Workflow ID'),
            "name": fields.String(description='Workflow name'),
        }))
    })))
})


workflow_failed_events_response_dto = api.inherit('Get Workflow Failed Events Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Failed Events',{
        "date": fields.String(description='Date of the event'),
        "error_code": fields.String(description='Error code'),
        "event_id": fields.String(description='Event ID'),
        "execution_id": fields.String(description='Execution ID'),
        "workflow": fields.Nested(api.model("Workflow", {
            "id": fields.String(description='Workflow ID'),
            "name": fields.String(description='Workflow Name'),
        }))
    })))
})


workflow_failures_response_dto = api.inherit('Get Workflow Failures Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Workflow Failures',{
        "workflow": fields.Nested(api.model("Workflow", {
            "id": fields.String(description='Workflow ID'),
            "name": fields.String(description='Workflow Name'),
        })),
        "errors": fields.List(fields.Nested(api.model("Workflow errors during execution", {
            "error_code": fields.String(description='Error code'),
            "occurrence": fields.Integer(description='Total occurrence of error'),
            "severity": fields.Float(description='Severity of the error.'),
        })))
    })))
})


@api.route("/stats")
class WorkflowStatsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Gets total number of executions and failed executions.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_stats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse the dates using ISO format
        try:
            start_date = datetime.fromisoformat(start_date_str)
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            log.error("Invalid date format. Use ISO format. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "Invalid date format. Use ISO format.")

        # Check if the date range is within 14 days
        if end_date - start_date >= timedelta(days=14):
            log.error("The date range cannot exceed 14 days. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "The date range cannot exceed 14 days.")

        user_data = g.get("user")
        user = User(**user_data)
        workflow_stats = dashboard_service.get_workflow_stats(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_stats), 200


@api.route("/executions")
class WorkflowExecutionMetricsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Gets total executions and total failed exeuctions by date..")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_execution_metrics_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse the dates using ISO format
        try:
            start_date = datetime.fromisoformat(start_date_str)
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            log.error("Invalid date format. Use ISO format. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "Invalid date format. Use ISO format.")

        # Check if the date range is within 14 days
        if end_date - start_date >= timedelta(days=14):
            log.error("The date range cannot exceed 14 days. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "The date range cannot exceed 14 days.")

        user_data = g.get("user")
        user = User(**user_data)
        workflow_execution_metrics = dashboard_service.get_workflow_execution_metrics_by_date(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_execution_metrics), 200


@api.route("/integrations")
class WorkflowIntegrationsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Integrations stats which includes workflow id, name, last event date, failure count and failure ratio.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_integrations_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse the dates using ISO format
        try:
            start_date = datetime.fromisoformat(start_date_str)
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            log.error("Invalid date format. Use ISO format. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "Invalid date format. Use ISO format.")

        # Check if the date range is within 14 days
        if end_date - start_date >= timedelta(days=14):
            log.error("The date range cannot exceed 14 days. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "The date range cannot exceed 14 days.")

        user_data = g.get("user")
        user = User(**user_data)
        workflow_integrations = dashboard_service.get_workflow_integrations(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_integrations), 200


@api.route("/failed-executions")
class WorkflowFailedExecutionsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Failed events stats which includes workflow details, event_id, execution_id & error_code.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_failed_events_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse the dates using ISO format
        try:
            start_date = datetime.fromisoformat(start_date_str)
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            log.error("Invalid date format. Use ISO format. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "Invalid date format. Use ISO format.")

        # Check if the date range is within 14 days
        if end_date - start_date >= timedelta(days=14):
            log.error("The date range cannot exceed 14 days. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "The date range cannot exceed 14 days.")

        user_data = g.get("user")
        user = User(**user_data)
        failed_executions = dashboard_service.get_workflow_failed_executions(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=failed_executions), 200
    

@api.route("/workflow-failures")
class WorkflowFailuresResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get workflow failures.")
    @api.expect(parser, validate=True)
    @api.marshal_with(workflow_failures_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse the dates using ISO format
        try:
            start_date = datetime.fromisoformat(start_date_str)
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            log.error("Invalid date format. Use ISO format. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "Invalid date format. Use ISO format.")

        # Check if the date range is within 14 days
        if end_date - start_date >= timedelta(days=14):
            log.error("The date range cannot exceed 14 days. api: %s, method: %s", request.url, request.method)
            raise ServiceException(400, ServiceStatus.FAILURE, "The date range cannot exceed 14 days.")

        user_data = g.get("user")
        user = User(**user_data)
        workflow_failures = dashboard_service.get_workflow_failures(user.organization_id, start_date, end_date)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflow_failures), 200
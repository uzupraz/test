from flask import g, request
from flask_restx import Namespace, Resource, fields
from dacite import from_dict

from configuration import AWSConfig, AppConfig
from repository import WorkflowRepository, DataStudioMappingRepository, DataFormatsRepository
from .server_response import ServerResponse
from .common_controller import server_response
from service import WorkflowService, DataStudioMappingService, DataFormatsService
from model import User, DataStudioSaveMapping
from enums import APIStatus


api = Namespace("Data Studio API", description="API for Data Studio", path="/interconnecthub/data-studio")
log = api.logger


app_config = AppConfig()
aws_config = AWSConfig()
workflow_repository = WorkflowRepository(app_config, aws_config)
data_studio_mapping_repository = DataStudioMappingRepository(app_config, aws_config)
data_formats_repository = DataFormatsRepository(app_config, aws_config)

data_studio_mapping_service = DataStudioMappingService(data_studio_mapping_repository=data_studio_mapping_repository)
workflow_service = WorkflowService(workflow_repository=workflow_repository)
data_formats_service = DataFormatsService(data_formats_repository=data_formats_repository)


# Model
mapping = api.model("Mapping", {
    "id": fields.String(description="The unique identifier of the mapping configuration"),
    "owner_id": fields.String(description="The unique identifier of the mapping's owner"),
    "revision": fields.Integer(description="The revision number of the mapping"),
    "status": fields.String(description="The current status of the mapping"),
    "active": fields.Boolean(description="Indicates if the mapping is currently active"),
    "created_by": fields.String(description="The user ID of the individual who created the mapping"),
    "created_at": fields.Integer(description="Timestamp of when the mapping was created"),
    "name": fields.String(description="The name of the mapping", required=False),
    "description": fields.String(description="A brief description of the mapping", required=False),
    "sources": fields.Raw(description="The source data for the mapping", required=False),
    "output": fields.Raw(description="The output configuration for the mapping", required=False),
    "mapping": fields.Raw(description="The mapping configuration details", required=False),
    "published_by": fields.String(description="The user ID of the individual who published the mapping", required=False),
    "published_at": fields.Integer(description="Timestamp of when the mapping was published", required=False),
    "version": fields.String(description="The version of the mapping", required=False),
    "tags": fields.String(description="The tags of the mapping for searching", required=False),
})

# DTO
data_studio_save_mapping_request_dto = api.model('Save data of user mapping draft', {
    "name": fields.String(description="The name of the mapping", required=False),
    "description": fields.String(description="A brief description of the mapping", required=False),
    "sources": fields.Raw(description="The source data for the mapping", required=False),
    "output": fields.Raw(description="The output configuration for the mapping", required=False),
    "mapping": fields.Raw(description="The mapping configuration details", required=False),
    "tags": fields.String(description="The tags of the mapping for searching", required=False),
})

data_studio_workflows_response_dto = api.inherit("Get workflows list", server_response, {
    "payload": fields.List(fields.Nested(api.model("Workflow", {
        "owner_id": fields.String(description="The unique identifier of the workflow's owner"),
        "workflow_id": fields.String(description="The unique identifier of the workflow"),
        "event_name": fields.String(description="The name of the event that triggers the workflow"),
        "created_by": fields.String(description="The user ID of the individual who created the workflow"),
        "created_by_name": fields.String(description="The name of the individual who created the workflow"),
        "last_updated": fields.String(description="Timestamp of the last update to the workflow"),
        "state": fields.String(description="The current state or status of the workflow"),
        "version": fields.Integer(description="The version number of the workflow"),
        "is_sync_execution": fields.Boolean(description="Indicates whether the workflow executes synchronously"),
        "state_machine_arn": fields.String(description="The AWS ARN of the state machine associated with the workflow"),
        "is_binary_event": fields.Boolean(description="Indicates whether the event triggering the workflow is binary"),
        "mapping_id": fields.String(description="The unique identifier of the mapping configuration for the workflow"),
    })))
})

data_studio_data_formats_response_dto = api.inherit("Get data format list", server_response, {
    "payload": fields.List(fields.Nested(api.model("DataFormat", {
        "format_id": fields.String(description="The unique identifier for the data format"),
        "name": fields.String(description="The name of the data format"),
        "parser": fields.Raw(description="Parser configuration as a dictionary with nested dictionaries"),
        "writer": fields.Raw(description="Writer configuration as a dictionary with nested dictionaries"),
    })))
})

data_studio_active_mappings_response_dto = api.inherit("Get mapping list", server_response, {
    "payload": fields.List(fields.Nested(mapping))
})

data_studio_mapping_response_dto = api.inherit("Get mapping revisions & draft", server_response, {
    "payload": fields.Nested(api.model("Mapping Draft & Revisions", {
        "draft": fields.Nested(mapping, description="The users active mapping draft", required=False),
        "revisions": fields.List(fields.Nested(mapping), description="The list of mapping revisions", required=True)
    }))
})


@api.route("/workflows")
class DataStudioWorkflowsResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get a list of workflows for the given owner where the mapping_id is present.")
    @api.marshal_with(data_studio_workflows_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get("user")
        user = User(**user_data)
        workflows = workflow_service.get_data_studio_workflows(user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflows), 200
    

@api.route("/data-formats")
class DataStudioDataFromatsResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get a list of available data formats for data studio.")
    @api.marshal_with(data_studio_data_formats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        workflows = data_formats_service.list_all_data_formats()
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=workflows), 200


@api.route("/mappings")
class DataStudioMappingsResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get list of active mappings")
    @api.marshal_with(data_studio_active_mappings_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get("user")
        user = User(**user_data)
        mappings = data_studio_mapping_service.get_active_mappings(user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=mappings), 200


    @api.doc(description="Create a new initial mapping that stores only the partial mapping entry and returns the mapping including partial values.")
    @api.marshal_with(data_studio_mapping_response_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get("user")
        user = User(**user_data)
        mapping_id = data_studio_mapping_service.create_mapping(user.sub, user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=mapping_id), 201


@api.route("/mappings/<string:mapping_id>")
class DataStudioMappingResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get revisions & draft of the mapping")
    @api.marshal_with(data_studio_mapping_response_dto, skip_none=True)
    def get(self, mapping_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get("user")
        user = User(**user_data)
        mappings = data_studio_mapping_service.get_mapping(user.organization_id, user.sub, mapping_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=mappings), 200


    @api.doc(description="Save user mapping draft")
    @api.expect(data_studio_save_mapping_request_dto, validate=True, skip_none=True)
    @api.marshal_with(server_response, skip_none=True)
    def patch(self, mapping_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get("user")
        user = User(**user_data)

        request.json['id'] = mapping_id
        mapping = from_dict(DataStudioSaveMapping, request.json)
        data_studio_mapping_service.save_mapping(user, mapping)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=None), 200


@api.route("/mappings/<string:mapping_id>/publish")
class PublishMappingResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Publishes the mapping and makes it active.")
    @api.marshal_with(data_studio_active_mappings_response_dto, skip_none=True)
    def post(self, mapping_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get('user')
        user = User(**user_data)
        published_mapping = data_studio_mapping_service.publish_mapping(user.sub, user.organization_id, mapping_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=published_mapping), 200

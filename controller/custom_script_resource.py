from flask_restx import Namespace, Resource, fields
from flask import g, request
from dacite import from_dict

from configuration import AWSConfig, AppConfig, S3AsssetFileConfig
from .server_response import ServerResponse
from .common_controller import server_response
from enums import APIStatus
from repository import CustomScriptRepository
from service import S3AssetsService, CustomScriptService
from model import User, CustomScriptRequestDTO


api = Namespace('Custom Script API', description='API for the working with s3 custom scripts', path='/interconnecthub/custom-scripts')
log = api.logger


app_config = AppConfig()
aws_config = AWSConfig()
s3_asset_file_config = S3AsssetFileConfig()

custom_script_repository = CustomScriptRepository(app_config, aws_config)
s3_assets_service = S3AssetsService(s3_asset_file_config)
custom_script_service = CustomScriptService(s3_assets_service=s3_assets_service, custom_script_repository=custom_script_repository)


# Models
releases = api.model('Custom script releases', {
    'version_id': fields.String(required=True),
    'edited_by': fields.String(required=True),
    'source_version_id': fields.String(required=True),
    'release_date': fields.Integer(required=True),
})
unpublished_changes = api.model('Custom script unpublished changes', {
    'version_id': fields.String(required=True),
    'edited_by': fields.String(required=True),
    'source_version_id': fields.String(required=False),
    'edited_at': fields.Integer(required=True),
})
custom_script = api.model('Custom script', {
    'owner_id': fields.String(required=True),
    'script_id': fields.String(required=True),
    'language': fields.String(required=True),
    'extension': fields.String(required=True),
    'name': fields.String(required=True),
    'releases': fields.List(fields.Nested(releases)),
    'unpublished_changes': fields.List(fields.Nested(unpublished_changes)),
    'creation_date': fields.Integer(description='Creation date')
})

# Request
save_custom_script_request_dto = api.model('Save custom script changes payload', {
    'script': fields.String(required=True),
    'script_id': fields.String(required=False),
    'source_version_id': fields.String(required=False),
    'is_sourced_from_release': fields.Boolean(required=False),
    'metadata': fields.Nested(api.model('Create custom script request payload', {
        'language': fields.String(required=True),
        'extension': fields.String(required=True),
        'name': fields.String(required=True)
    }), allow_null=True)
})

# Responses
custom_script_response_dto = api.inherit('Save custom script response',server_response, {
    'payload': fields.Nested(unpublished_changes)
})

custom_script_content_response_dto = api.inherit('Custom script content response',server_response, {
    'payload': fields.String(description='Content of the file')
})

custom_script_release_response_dto = api.inherit('Custom script release response',server_response, {
    'payload': fields.Nested(releases)
})

get_custom_scripts_response_dto = api.inherit('Get custom scripts response',server_response, {
    'payload': fields.List(fields.Nested(custom_script))
})


@api.route('')
class CustomScriptResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='Save custom script if does not exist else creates unpublished change')
    @api.expect(save_custom_script_request_dto, description='Custom script information')
    @api.marshal_with(custom_script_response_dto, skip_none=True)
    def put(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        payload = from_dict(CustomScriptRequestDTO, request.json)
        response_payload = custom_script_service.save_custom_script(
            owner_id=user.organization_id,
            payload=payload
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 201


    @api.doc(description='Get custom scripts')
    @api.marshal_with(get_custom_scripts_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        response_payload = custom_script_service.get_custom_scripts(
            owner_id=user.organization_id,
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
    

@api.route('/<string:script_id>')
class CustomScriptDeleteResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='Delete unpublished change')
    @api.marshal_with(server_response, skip_none=True)
    def delete(self, script_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        custom_script_service.remove_unpublished_change(
            owner_id=user.organization_id,
            script_id=script_id,
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=None), 200


@api.route('/<string:script_id>/contents')
class CustomScriptContentResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='Get custom script content excluding others unpublished changes.')
    @api.param('branch', 'Get from release or unpublished ', type=str, default='unpublished')
    @api.param('version_id', 'Get specific version ', type=str, default=None)
    @api.marshal_with(custom_script_content_response_dto, skip_none=True)
    def get(self, script_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        branch = request.args.get('branch', type=str, default='unpublished')
        version_id = request.args.get('version_id', type=str)
        
        user = from_dict(User, g.get('user'))

        response_payload = custom_script_service.get_custom_script_content(
            owner_id=user.organization_id,
            script_id=script_id,
            from_release=branch == 'release',
            version_id=version_id
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
  

@api.route('/<string:script_id>/release')
class CustomScriptReleaseResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='Releases unchanged custom script of the current user')
    @api.marshal_with(custom_script_content_response_dto, skip_none=True)
    def post(self, script_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        response_payload = custom_script_service.release_custom_script(
            owner_id=user.organization_id,
            script_id=script_id
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200

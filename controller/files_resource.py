from flask_restx import fields, Resource, Namespace
from flask import request

from .server_response import ServerResponse
from .common_controller import server_response
from configuration import AWSConfig, AppConfig
from service import ProcessorTemplateService
from repository import ProcessorTemplateRepo
from enums import APIStatus


api = Namespace('Files API', description='Manages operations related to files.', path='/files')
log = api.logger

file_dto = api.model('Processor Template', {
    'filename': fields.String(required=True),
    'url': fields.String(required=True)
})

list_files_dto = api.inherit('Listed Files', server_response, {
       'payload': fields.List(fields.Nested(file_dto))
})

request_file_upload_dto = api.model('Request File Upload', {
    'owner_id': fields.String(required=True),
    'path': fields.String(required=True)
})

request_file_upload_response_dto = api.inherit('Pre Signed URL Response Model', server_response, {
        'payload': fields.Nested(api.model('URL Dictionary', {
        'url': fields.String
    }))
})

@api.route('')
class FilesResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='List all files in a defined folder', params={'path': {'in': 'query', 'description': 'The path for which the files should be listed.', 'type': 'string', 'required': True}})
    @api.marshal_with(list_files_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=[]), 200
    
    
    @api.doc(description='Gets the pre-signed url from S3 for async file delivery')
    @api.expect(request_file_upload_dto, validate=True)
    @api.marshal_with(request_file_upload_response_dto, skip_none=True)
    def post(self):
        return ServerResponse.success(payload=None), 200
    

    @api.doc(description='Update file state')
    @api.expect(request_file_upload_dto, validate=True)
    @api.marshal_with(server_response, skip_none=True)
    def put(self):
        return ServerResponse.success(payload=None), 200
from flask_restx import fields, Resource, Namespace
from flask import request

from .server_response import ServerResponse
from .common_controller import server_response
from enums import APIStatus
from service import S3FileService
from configuration import AsyncFileDeliveryS3Config


api = Namespace('Files API', description='Manages operations related to async submission of files.', path='/files')
log = api.logger

file_dto = api.model('Processor Template', {
    'path': fields.String(required=True),
    'url': fields.String(required=True)
})

response_list_files_dto = api.inherit('Listed Files', server_response, {
    'payload': fields.List(fields.Nested(file_dto))
})

request_file_upload_dto = api.model('Request File Upload', {
    'owner_id': fields.String(required=True),
    'path': fields.String(required=True)
})

upload_file_dto = api.model('Approved File Upload', {
    'url': fields.String
})

response_file_upload_dto = api.inherit('Pre Signed URL Response Model', server_response, {
        'payload': fields.Nested(upload_file_dto)
})

@api.route('')
class FilesResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.file_delivery_config = AsyncFileDeliveryS3Config()
        self.s3_file_service = S3FileService(self.file_delivery_config)


    @api.doc(description='List all files in a defined folder', params={
            'path': {'in': 'query', 'description': 'The path for which the files should be listed.', 'type': 'string', 'required': True},
            'owner': {'in': 'query', 'description': 'The owner id for which the files should be listed', 'type': 'string', 'required': True}
        })
    @api.marshal_with(response_list_files_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        owner_id = request.args.get('owner')
        relative_path = request.args.get('path')
        result = self.s3_file_service.list_files_in_output_folder(owner_id, relative_path)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=result), 200
    
    
    @api.doc(description='Gets the pre-signed url from S3 for async file delivery')
    @api.expect(request_file_upload_dto, validate=True)
    @api.marshal_with(response_file_upload_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        request_body = api.payload
        pre_signed_url = self.s3_file_service.generate_upload_pre_signed_url(request_body['owner_id'], request_body['path'])
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload={"url": pre_signed_url}), 200
    

@api.route('/confirm-download')
class ConfirmDownloadResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.file_delivery_config = AsyncFileDeliveryS3Config()
        self.s3_file_service = S3FileService(self.file_delivery_config)

    
    @api.doc(description='Update file state')
    @api.expect(request_file_upload_dto, validate=True)
    @api.marshal_with(server_response, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        request_body = api.payload
        self.s3_file_service.archive_output_file(request_body['owner_id'], request_body['path'])
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=None), 200
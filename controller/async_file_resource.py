from flask_restx import fields, Resource, Namespace
from flask import request

from .server_response import ServerResponse
from .common_controller import server_response
from enums import APIStatus
from service import S3FileService
from configuration import AsyncFileDeliveryS3Config


api = Namespace('Async File Delivery API', description='Manages operations related to async file delivery.', path='/files')
log = api.logger

pre_signed_url_request_dto = api.model('Pre Signed URL Request Model', {
    'owner_id': fields.String(required=True),
    'filename': fields.String(required=True),
})

pre_signed_url_response_dto = api.inherit('Returns Pre Signed URL', server_response, {
       'payload': fields.String
})

@api.route('')
class AsyncFileResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.file_delivery_config = AsyncFileDeliveryS3Config()
        self.s3_file_service = S3FileService(self.file_delivery_config)


    @api.doc('Gets the pre-signed url from S3 for async file delivery')
    @api.expect(pre_signed_url_request_dto, validate=True)
    @api.marshal_with(pre_signed_url_response_dto, skip_none=True)
    @api.route('/pre-signed-url', methods=['GET'])
    def get_pre_signed_url(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        request_body = api.payload
        pre_signed_url = self.s3_file_service.get_pre_signed_url(request_body['owner_id'], request_body['filename'])
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.success(payload=pre_signed_url), 200
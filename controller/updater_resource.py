from flask_restx import fields, Resource, Namespace
from flask import request

from .server_response import ServerResponse
from .common_controller import target_list_dto,server_response
from configuration import AWSConfig, AppConfig, S3AssetsFileConfig
from service import UpdaterService
from repository import UpdaterRepository
from enums import APIStatus

api = Namespace('Update API ', description='Manages operation related to updater.', path='/interconnecthub/updates')
log=api.logger

update_response_dto = api.inherit('Update Response',server_response,{
    'target_list':fields.List(fields.Nested(target_list_dto))
})

@api.route('')
class UpdaterResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.s3_assets_file_config = S3AssetsFileConfig()
        self.updater_repository = UpdaterRepository(self.app_config, self.aws_config)
        self.updater_service = UpdaterService(self.updater_repository, self.s3_assets_file_config)

    @api.doc('Check for available updates')
    @api.marshal_with(update_response_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        try:
            data = request.json
            owner_id =  data['owner_id']
            machine_id = data['machine_id']
            module_list = data['modules']
            
            update_response = self.updater_service.get_target_list(machine_id, owner_id, module_list)  
            log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
            return ServerResponse.success(update_response), 200

        except KeyError as e:
            log.error("Missing key in request body: %s, api: %s, method: %s", str(e), request.url, request.method)
            return ServerResponse.error('Missing required parameters.'), 400  

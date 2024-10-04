from flask_restx import fields, Resource, Namespace
from flask import g, request
from dacite import from_dict

from ..server_response import ServerResponse
from ..common_controller import target_list_dto,server_response
from configuration import AWSConfig, AppConfig, S3AssetsFileConfig
from service import CsaUpdaterService
from repository import CsaUpdaterRepository
from enums import APIStatus
from model import User,UpdateRequest


api = Namespace('Update API ', description='Manages operation related to updater.', path='/interconnecthub/updates')
log=api.logger


update_response_dto = api.inherit('Update Response',server_response,{
    'target_list':fields.List(fields.Nested(target_list_dto))
})


@api.route('')
class CsaUpdaterResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.s3_assets_file_config = S3AssetsFileConfig()
        self.csa_updater_repository = CsaUpdaterRepository(self.app_config, self.aws_config)
        self.csa_updater_service = CsaUpdaterService(self.csa_updater_repository, self.s3_assets_file_config)


    @api.doc('Check for available updates')
    @api.marshal_with(update_response_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))
        
        request_data = UpdateRequest(
            machine_id=api.payload['machine_id'],
            modules=api.payload['modules']
        )

        response_payload = self.csa_updater_service.get_target_list(
            owner_id=user.organization_id, 
            machine_id=request_data.machine_id, 
            modules=request_data.modules
        )  
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(response_payload), 200

    

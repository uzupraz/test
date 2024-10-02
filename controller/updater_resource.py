from flask_restx import fields, Resource, Namespace
from flask import g, request
from dacite import from_dict

from .server_response import ServerResponse
from .common_controller import target_list_dto,server_response
from configuration import AWSConfig, AppConfig, S3AssetsFileConfig
from service import UpdaterService
from repository import UpdaterRepository
from enums import APIStatus, ServicePermissions, ServiceStatus
from exception import ServiceException
from model import User

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
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        
        request_body = api.payload
        machine_id = request_body['machine_id']
        machine_module_list = request_body['modules']

        if not user.has_permission(ServicePermissions.UPDATER_GET_TARGET_LIST.value):
            log.warning('User has no permission to get target list. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE.value)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to get target list')
            
        response_payload = self.updater_service.get_target_list(
            owner_id=user.organization_id, 
            machine_id=machine_id, 
            machine_module_list=machine_module_list
        )  
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(response_payload), 200

    

   
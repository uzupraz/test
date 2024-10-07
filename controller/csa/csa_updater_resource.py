from flask_restx import fields, Resource, Namespace
from flask import g, request
from dacite import from_dict

from controller.server_response import ServerResponse
from controller.common_controller import targets_dto, server_response
from configuration import AWSConfig, AppConfig, S3AssetsFileConfig
from service import CsaUpdaterService
from repository import CsaMachinesRepository, CsaModuleVersionsRepository
from enums import APIStatus
from model import User,UpdateRequest


api = Namespace('CSA Updater API ', description='API for updating CSA modules in client side.', path='/interconnecthub/updates')
log=api.logger


update_response_dto = api.inherit('Update Response',server_response,{
    'targets':fields.List(fields.Nested(targets_dto))
})

update_request_dto = api.model('UpdateRequest', {
    'machine_id': fields.String(required=True, description='ID of the machine'),
    'modules': fields.List(fields.String, required=True, description='List of modules to be updated')
})


@api.route('')
class CsaUpdaterResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.s3_assets_file_config = S3AssetsFileConfig()
        self.csa_machines_repository = CsaMachinesRepository(self.app_config, self.aws_config)
        self.csa_module_versions_repository = CsaModuleVersionsRepository(self.app_config, self.aws_config)
        self.csa_updater_service = CsaUpdaterService(self.csa_machines_repository, self.csa_module_versions_repository, self.s3_assets_file_config)


    @api.doc('Check for available updates')
    @api.expect(update_request_dto, description='Machine module information')
    @api.marshal_with(update_response_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))
        
        request_data = UpdateRequest(
            owner_id=user.organization_id,
            machine_id=api.payload['machine_id'],
            modules=api.payload['modules']
        )

        response_payload = self.csa_updater_service.get_targets(
            owner_id=user.organization_id, 
            machine_id=request_data.machine_id, 
            machine_modules=request_data.modules
        )  
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(response_payload), 200

    

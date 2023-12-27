from flask_restx import fields, Resource, Namespace
from flask import request

from .server_response import ServerResponse
from .common_controller import processor_template_dto, server_response
from configuration import AWSConfig, AppConfig
from service import ProcessorTemplateService
from repository import ProcessorTemplateRepo
from enums import APIStatus


api = Namespace('Processor Template API', description='Manages operations related to processor template.', path='/processors')
log = api.logger

list_processor_templates_response_dto = api.inherit('List Processor Response', server_response, {
       'payload': fields.List(fields.Nested(processor_template_dto))
})

@api.route('/')
class ProcessorTemplateResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.processor_template_repo = ProcessorTemplateRepo(self.app_config, self.aws_config)
        self.processor_template_service = ProcessorTemplateService(self.processor_template_repo)


    @api.doc('List all Processor Templates')
    @api.marshal_with(list_processor_templates_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START)
        processor_templates = self.processor_template_service.get_all_templates()
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS)
        return ServerResponse.created(payload=processor_templates), 200
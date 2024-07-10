from flask import g, request
from flask_restx import Namespace, Resource, fields

from configuration import AWSConfig, AppConfig
from .server_response import ServerResponse
from .common_controller import server_response
from service import DataStudioService
from model import User
from enums import APIStatus


api = Namespace("Data Studio API", description="API for Data Studio", path="/interconnecthub/data-studio")
log = api.logger


app_config = AppConfig()
aws_config = AWSConfig()
data_studio_service = DataStudioService()


field_dto = api.model('Field', {
    "name": fields.String(description="Name of the field"),
    "type": fields.String(description="Type of the field"),
    "subtype": fields.String(description="Subtype of the field if type is an `ARRAY`"),
    "operation": fields.String(description="Defines whether if this is an operation or not"),
    "mappedTo": fields.String(description="A Relative location in the input schema. Note that if this field is part of an operation, then the input location is relative to the `mappedTo` path of the Parent else it is the relative path from the root of the Input."),
})


data_studio_mappings_response_dto = api.inherit("Get mappings list", server_response, {
    "payload": fields.List(fields.Nested(api.model("Mappings", {
        "name": fields.String(description="Fixed value for root element"),
        "type": fields.String(description="Type of the mapping"),
        "subtype": fields.String(description="Subtype of the mapping if type is an `ARRAY`"),
        "fields": fields.List(fields.Nested(field_dto)),
    })))
})


@api.route("/mappings")
class DataStudioMappingsResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)
    

    @api.doc(description="Get list of mappings")
    @api.marshal_with(data_studio_mappings_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user_data = g.get("user")
        user = User(**user_data)
        mappings = data_studio_service.get_mappings(user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=mappings), 200
    
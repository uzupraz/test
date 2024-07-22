from flask_restx import Namespace, Resource, fields
from flask import g, request

from configuration import AWSConfig, AppConfig
from repository import CustomerTableInfoRepository
from .server_response import ServerResponse
from service import DataTableService
from .common_controller import server_response
from enums import APIStatus
from model import User

api = Namespace(
    name="Data Table API",
    description="API for managing customer tables within the Interconnect Hub. This API provides endpoints for creating, retrieving, updating, and deleting data tables, as well as fetching detailed information and statistics about them.",
    path="/interconnecthub/data-table"
)
log = api.logger

aws_config = AWSConfig()
app_config = AppConfig()
customer_table_info_repository = CustomerTableInfoRepository(app_config=app_config, aws_config=aws_config)
data_table_service = DataTableService(customer_table_info_repository=customer_table_info_repository)

tables_response_dto = api.inherit('Customer tables response',server_response, {
    'payload': fields.List(fields.Nested(api.model('List of customer tables', {
        "name": fields.String(description='Name of the table'),
        "id": fields.String(description='Id of the table'),
        "size": fields.Float(description='Size of the table in Kilo-bytes'),
    })))
})

update_description_dto = api.model('Update table', {
    'description': fields.String(required=True, description='The new description for the table')
})

@api.route('/tables')
class DataTableResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get the list of tables belonging to the logged in user.")
    @api.marshal_with(tables_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = g.get("user")
        user = User(**user)
        tables = data_table_service.list_tables(owner_id=user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=tables), 200


@api.route('/tables/<string:table_id>/update-table')
class UpdateTableDescriptionAction(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Update the fields of a table.")
    @api.expect(update_description_dto, validate=True)
    @api.marshal_with(server_response, skip_none=True)
    def patch(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = g.get("user")
        user = User(**user)
        description = request.json['description']
        data_table_service.update_table(owner_id=user.organization_id, table_id=table_id, description=description)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(), 201

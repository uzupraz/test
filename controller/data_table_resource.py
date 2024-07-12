from flask_restx import Namespace, Resource, fields
from flask import g, request

from configuration import AWSConfig, AppConfig
from repository import CustomerTableInfoRepository
from .server_response import ServerResponse
from service import DataTableService
from .common_controller import server_response
from enums import APIStatus
from model import User

api = Namespace("Data table API", description="API for the data table", path="/interconnecthub/data-table")
log = api.logger

aws_config = AWSConfig()
app_config = AppConfig()
customer_table_info_repository = CustomerTableInfoRepository(app_config=app_config, aws_config=aws_config)
data_table_service = DataTableService(customer_table_info_repository=customer_table_info_repository)

tables_response_dto = api.inherit('Data tables Response',server_response, {
    'payload': fields.List(fields.Nested(api.model('Data tables', {
        "name": fields.String(description='Name of the table'),
        "id": fields.String(description='Id of the table'),
        "size": fields.Integer(description='Size of the table in Kilo-bytes'),
    })))
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

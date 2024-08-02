from flask_restx import Namespace, Resource, fields
from flask import g, request

from configuration import AWSConfig, AppConfig
from repository import CustomerTableInfoRepository
from .server_response import ServerResponse
from service import DataTableService
from .common_controller import server_response
from enums import APIStatus
from model import User, UpdateTableRequest

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

list_tables_response_dto = api.inherit('List customer tables response',server_response, {
    'payload': fields.List(fields.Nested(api.model('List of customer tables', {
        "name": fields.String(description='Name of the table'),
        "id": fields.String(description='Id of the table'),
        "size": fields.Float(description='Size of the table in Kilo-bytes'),
    })))
})

update_table_request_dto = api.model('Update customer table request', {
    'description': fields.String(required=True, description='The description to update in the table')
})

update_table_response_dto = api.inherit('Update customer table response',server_response, {
    'payload': fields.Nested(api.model('Updated customer table info', {
        'table_id': fields.String(required=True, description='Id of the table'),
        'table_name': fields.String(required=True, description='Name of the table'),
        'description': fields.String(required=True, description='Description of the table'),
        'created_by': fields.String(required=True, description='Creator of the table'),
        'creation_time': fields.String(required=True, description='Table creation date time'),
        'total_indexes': fields.String(required=True, description='Total indexes of the table'),
        'read_capacity_units': fields.String(required=True, description='Read capacity units of the table'),
        'write_capacity_units': fields.String(required=True, description='Write capacity units of the table'),
        'backups': fields.String(required=True, description='Backup status of the table'),
        'table_status': fields.String(required=True, description='Status of the table'),
        'alarms': fields.String(required=True, description='Alarm status of the table'),
        'next_backup_schedule': fields.String(required=True, description='The next backup schedule date time of the table'),
        'last_backup_schedule': fields.String(required=True, description='The last backup schedule date time of the table')
    }))
})

table_details_response_dto = api.inherit('Customer table details response',server_response, {
    'payload': fields.Nested(api.model('Customer table details', {
        'table_id': fields.String(required=True, description='Id of the table'),
        'table_name': fields.String(required=True, description='Name of the table'),
        'description': fields.String(description='Description of the table'),
        'created_by': fields.String(description='Creator of the table'),
        'creation_time': fields.String(description='Table creation date time'),
        'total_indexes': fields.Integer(description='Total indexes of the table'),
        'read_capacity_units': fields.Integer(description='Read capacity units of the table'),
        'write_capacity_units': fields.Integer(description='Write capacity units of the table'),
        'backups': fields.String(description='Backup status of the table'),
        'table_status': fields.String(description='Status of the table'),
        'alarms': fields.String(description='Alarm status of the table'),
        'next_backup_schedule': fields.String(description='The next backup schedule date time of the table'),
        'last_backup_schedule': fields.String(description='The last backup schedule date time of the table'),
        'indices': fields.List(fields.Nested(api.model('Index Info', {
            'name': fields.String(description='Name of the index'),
            'status': fields.String(description='Status of the index'),
            'partition_key': fields.String(description='Partition key of the index'),
            'sort_key': fields.String(description='Sort key of the index'),
            'size': fields.Integer(description='Size of the index in KB'),
            'item_count': fields.Integer(description='Item count of the index')
        })))
    }))
})

list_backups_response_dto = api.inherit('List of Backups Response', server_response, {
    'payload': fields.List(fields.Nested(api.model('Backup List', {
        'name': fields.String(description='Name of the backup'),
        'status': fields.String(description='Status of the backup'),
        'creation_time': fields.String(description='Backup creation date time'),
        'type': fields.String(description='Type of the backup'),
        'size': fields.Integer(description='Size of the backup in KB')
    })))
})

@api.route('/tables')
class DataTableListResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get the list of tables belonging to the logged in user.")
    @api.marshal_with(list_tables_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = User(**g.get("user"))
        tables = data_table_service.list_tables(owner_id=user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=tables), 200


@api.route('/tables/<string:table_id>')
class DataTableResource (Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Update the fields in customer table.")
    @api.expect(update_table_request_dto, validate=True)
    @api.marshal_with(update_table_response_dto, skip_none=True)
    def put(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = User(**g.get("user"))
        update_table_request = UpdateTableRequest(**request.json)
        updated_customer_table_info = data_table_service.update_table(owner_id=user.organization_id, table_id=table_id, update_table_request=update_table_request)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=updated_customer_table_info), 200


    @api.doc(description="Get the details of a specific table by its ID.")
    @api.marshal_with(table_details_response_dto, skip_none=True)
    def get(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = User(**g.get("user"))
        table_details = data_table_service.get_table_details(owner_id=user.organization_id, table_id=table_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=table_details), 200


@api.route('/tables/<string:table_id>/backups')
class TableBackupsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    @api.doc(description="Get the list of backups for a specific table by its ID.")
    @api.marshal_with(list_backups_response_dto, skip_none=True)
    def get(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = User(**g.get("user"))
        backups = data_table_service.get_table_backups(owner_id=user.organization_id, table_id=table_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=backups), 200

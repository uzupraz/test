from flask_restx import Namespace, Resource, fields
from flask import g, request
from dacite import from_dict

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

table_info_response_dto = api.inherit('Customer table info response',server_response, {
    'payload': fields.Nested(api.model('Customer table info', {
        'owner_id': fields.String(required=True, description='Owner id of the table'),
        'table_id': fields.String(required=True, description='Id of the table'),
        'table_name': fields.String(required=True, description='Name of the table'),
        'partition_key': fields.String(required=True, description='Partition key of the table'),
        'sort_key': fields.String(required=True, description='Sort key of the table'),
        'description': fields.String(required=True, description='Description of the table'),
        'created_by': fields.String(required=True, description='Creator of the table'),
        'creation_time': fields.String(required=True, description='Table creation date time'),
        'total_indexes': fields.String(required=True, description='Total indexes of the table'),
        'read_capacity_units': fields.String(required=True, description='Read capacity units of the table'),
        'write_capacity_units': fields.String(required=True, description='Write capacity units of the table'),
        'backup': fields.String(required=True, description='Backup state of the table'),
        'auto_backup_status': fields.String(required=True, description='Auto backup status of the table'),
        'table_status': fields.String(required=True, description='Status of the table'),
        'backup_schedule': fields.String(required=True, description='The backup schedule cron pattern'),
        'table_arn': fields.String(required=True, description='The ARN of the table'),
        'indexes': fields.List(fields.Nested(api.model('Index Info', {
            'name': fields.String(description='Name of the index'),
            'status': fields.String(description='Status of the index'),
            'partition_key': fields.String(description='Partition key of the index'),
            'sort_key': fields.String(description='Sort key of the index'),
            'size': fields.Integer(description='Size of the index in KB'),
            'item_count': fields.Integer(description='Item count of the index')
        })))
    }))
})

backups_response_dto = api.inherit('List of Backup Response', server_response, {
    'payload': fields.List(fields.Nested(api.model('Backup List', {
        'id': fields.String(descripiotn='Id of the backup'),
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
        user = from_dict(User, g.get('user'))
        tables = data_table_service.list_tables(owner_id=user.organization_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=tables), 200


@api.route('/tables/<string:table_id>')
class DataTableResource (Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Update the description in customer table and returns the updated table info.")
    @api.expect(update_table_request_dto, validate=True)
    @api.marshal_with(table_info_response_dto, skip_none=True)
    def put(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = from_dict(User, g.get('user'))
        update_table_request = from_dict(UpdateTableRequest, request.json)
        updated_customer_table_info = data_table_service.update_description(owner_id=user.organization_id, table_id=table_id, update_table_request=update_table_request)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=updated_customer_table_info), 200


    @api.doc(description="Get the info of a specific table by its ID.")
    @api.marshal_with(table_info_response_dto, skip_none=True)
    def get(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = from_dict(User, g.get('user'))
        table_details = data_table_service.get_table_info(owner_id=user.organization_id, table_id=table_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=table_details), 200


@api.route('/tables/<string:table_id>/backups')
class TableBackupsResource(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    @api.doc(description="Get the list of backup for a specific table by its ID.")
    @api.marshal_with(backups_response_dto, skip_none=True)
    def get(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = User(**g.get("user"))
        backups = data_table_service.get_table_backups(owner_id=user.organization_id, table_id=table_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=backups), 200

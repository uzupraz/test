from flask_restx import Namespace, Resource, fields
from flask import g, request
from dacite import from_dict

from configuration import AWSConfig, AppConfig
from repository import CustomerTableInfoRepository, CustomerTableRepository
from .server_response import ServerResponse
from service import DataTableService
from .common_controller import server_response
from enums import APIStatus
from model import User, UpdateTableRequest
from exception import ServiceException
from enums import ServiceStatus, ServicePermissions
from utils import Base64ConversionUtils

api = Namespace(
    name="Data Table API",
    description="API for managing customer tables within the Interconnect Hub. This API provides endpoints for creating, retrieving, updating, and deleting data tables, as well as fetching detailed information and statistics about them.",
    path="/interconnecthub/data-table"
)
log = api.logger

aws_config = AWSConfig()
app_config = AppConfig()
customer_table_info_repository = CustomerTableInfoRepository(app_config=app_config, aws_config=aws_config)
customer_table_repository = CustomerTableRepository(app_config=app_config, aws_config=aws_config)
data_table_service = DataTableService(
    customer_table_info_repository=customer_table_info_repository,
    customer_table_repository=customer_table_repository
)

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

customer_table_item_response_dto = api.inherit('Table item response',server_response, {
    'payload': fields.Nested(api.model('Table items', {
        'items': fields.List(fields.Raw(description='Item'), description='List of items'),
        'pagination': fields.Nested(api.model('Pagination parameters', {
            'size': fields.Integer(description='Items per page', required=True),
            'last_evaluated_key': fields.String(description="A key which was evaluated in previous request & will be used as exclusive start key in current request", required=False)
        }), skip_none=True)
    }))
})

customer_table_create_item_request_dto = fields.Raw(description='The item to create')

customer_table_create_item_response_dto = api.inherit('Create item response',server_response, {
    'payload': fields.Raw(description='The created item')
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


    @api.doc(description='Create')
    @api.expect(customer_table_create_item_request_dto, description='The item to create')
    @api.marshal_with(customer_table_create_item_response_dto, skip_none=True)
    def post(self, table_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))
        item = request.json

        if not user.has_permission(ServicePermissions.DATA_TABLE_CREATE_ITEM.value):
            log.warn('User has no permission to create item in table. api: %s, method: %s, status: %s, table_id: %s', request.url, request.method, APIStatus.FAILURE.value, table_id)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to create item in table')
        
        response_payload = data_table_service.create_item(
            owner_id=user.organization_id,
            table_id=table_id,
            item=item
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 201


@api.route('/tables/<string:table_id>/backups')
class TableBackupsResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description="Get the list of backup jobs for a specific table by its ID.")
    @api.marshal_with(backups_response_dto, skip_none=True)
    def get(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)
        user = from_dict(User, g.get('user'))
        backups = data_table_service.get_table_backup_jobs(owner_id=user.organization_id, table_id=table_id)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=backups), 200


@api.route('/tables/<string:table_id>/items')
class DataTableItemsResource (Resource):
    
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='Get the table items of the provided table id.')
    @api.param('size', 'Number of items to retrieve', type=int, default=10)
    @api.param('last_evaluated_key', 'Pagination key for the next set of items', type=str)
    @api.marshal_with(customer_table_item_response_dto, skip_none=True)
    def get(self, table_id:str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        size = request.args.get('size', type=int)
        last_evaluated_key = request.args.get('last_evaluated_key', default=None, type=str)
        user = from_dict(User, g.get('user'))

        response_payload = data_table_service.get_table_items(
            owner_id=user.organization_id,
            table_id=table_id,
            size=size,
            last_evaluated_key=last_evaluated_key
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200


@api.route('/tables/<string:table_id>/query')
class DataTableItemResource (Resource):
    
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    
    @api.doc(description='Query items from the specified table using partition and sort keys, with optional attribute filters.')
    @api.param('partition_key_value', 'The value of the partition key to query', type=str, required=True)
    @api.param('sort_key_value', 'The value of the sort key to query (optional)', type=str)
    @api.param('attribute_filters', 'JSON object with attribute filters (optional)', type=str)
    @api.marshal_with(customer_table_item_response_dto, skip_none=True)
    def get(self, table_id: str):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        # Extract parameters from request arguments
        partition_key_value = request.args.get('partition_key_value', type=str)
        sort_key_value = request.args.get('sort_key_value', type=str)
        
        if not partition_key_value:
            log.warn('Missing partition_key_value in query. api: %s, method: %s, status: %s, table_id: %s', request.url, request.method, APIStatus.FAILURE.value, table_id)
            raise ServiceException(400, ServiceStatus.FAILURE, 'Missing partition_key_value in query')
        
        # Parse attribute filters from base64 string to dictionary
        attribute_filters = request.args.get('attribute_filters', type=str)
        attribute_filters = Base64ConversionUtils.decode_to_dict(attribute_filters) if attribute_filters else None

        user = from_dict(User, g.get('user'))

        response_payload = data_table_service.query_item(
            owner_id=user.organization_id,
            table_id=table_id,
            partition_key_value=partition_key_value,
            sort_key_value=sort_key_value,
            attribute_filters=attribute_filters
        )

        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200


@api.route('/tables/<string:table_id>/items/<string:partition_key>')
class DataTableItemResource (Resource):
    
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc(description='Delete an item from the table using the partition key and sort key.')
    @api.param('sort_key', 'Sort key', type=str)
    @api.marshal_with(server_response, skip_none=True)
    def delete(self, table_id: str, partition_key: str):
        log.info('Received API Request for deletion. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        sort_key = request.args.get('sort_key', default=None, type=str)
        user = from_dict(User, g.get('user'))

        if not user.has_permission(ServicePermissions.DATA_TABLE_DELETE_ITEM.value):
            log.warning('User has no permission to delete item in table. api: %s, method: %s, status: %s, table_id: %s', request.url, request.method, APIStatus.FAILURE.value, table_id)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to delete item in table')

        data_table_service.delete_item(
            owner_id=user.organization_id,
            table_id=table_id,
            partition_key_value=partition_key,
            sort_key_value=sort_key
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.response(
            code=ServiceStatus.SUCCESS,
            message='Successfully deleted item from table',
            payload=None
        ), 200
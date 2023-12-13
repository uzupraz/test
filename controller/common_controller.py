from flask_restx import  fields, Resource, Namespace

from .server_response import ServerResponse


health_api = Namespace('Health API', description='Checks the health of an API', path='/health')
log = health_api.logger

server_response = health_api.model('Server Response', {
    'code': fields.String,
    'message': fields.String,
    'payload': fields.Raw,
    'timestamp': fields.DateTime
})

connection_dto = health_api.model('Connection', {
    'source_node': fields.String(required=True),
    'target_node': fields.String(required=True)
})

config_dto = health_api.model('Config', {
    'start_at': fields.String(required=True),
    'connections': fields.List(fields.Nested(connection_dto))
})

sub_workflow_dto = health_api.model('SubWorkflow', {
    'config': fields.Nested(config_dto)
})

node_dto = health_api.model('Node', {
    'id': fields.String(required=True),
    'name': fields.String(required=True),
    'description': fields.String(required=True),
    'type': fields.String(required=True),
    'has_subflow': fields.Boolean(required=True),
    'parameters': fields.Raw(required=True),
    'node_template_id': fields.String(required=True),
    'sub_workflow': fields.Nested(sub_workflow_dto, allow_null=True)
})

# Add nodes to config after node is defined
config_dto['nodes'] = fields.List(fields.Nested(node_dto))

workflow_dto = health_api.model('Workflow', {
    'owner_id': fields.String(required=True),
    'workflow_id': fields.String(required=True),
    'config': fields.Nested(config_dto),
    'created_by': fields.String(required=True),
    'created_by_name': fields.String(required=True),
    'creation_date': fields.String(required=True),
    'group_name': fields.String(required=True),
    'name': fields.String(required=True),
    'state': fields.String(required=True),
    'workflow_version': fields.Integer(required=True),
    'schema_version': fields.Integer(required=True)
})


@health_api.route('/')
class HealthResource(Resource):

    @health_api.marshal_with(server_response, skip_none=True)
    def get(self):
        return ServerResponse.success(), 200
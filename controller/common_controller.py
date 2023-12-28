from flask_restx import  fields, Resource, Namespace

from .server_response import ServerResponse


api = Namespace('Health API', description='Checks the health of an API', path='/health')
log = api.logger

server_response = api.model('Server Response', {
    'request_id': fields.String,
    'code': fields.String,
    'message': fields.String,
    'payload': fields.Raw,
    'timestamp': fields.DateTime
})

connection_dto = api.model('Connection', {
    'source_node': fields.String(required=True),
    'target_node': fields.String(required=True)
})

config_dto = api.model('Config', {
    'start_at': fields.String(required=True),
    'connections': fields.List(fields.Nested(connection_dto))
})

sub_workflow_dto = api.model('SubWorkflow', {
    'config': fields.Nested(config_dto)
})

node_dto = api.model('Node', {
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

workflow_dto = api.model('Workflow', {
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

# Processor Template Related DTO Items

input_description_dto = api.model('Template Input Description', {
    'description': fields.String(),
    'format': fields.String(),
    'media_type': fields.String()
})

output_description_dto = api.model('Template Output Description', {
    'description': fields.String(),
    'format': fields.String(),
    'media_type': fields.String()
})

parameter_description_dto = api.model('Parameter Description', {
    'parameter_id': fields.String(required=True),
    'description': fields.String(required=True),
    'name': fields.String(required=True),
    'order': fields.Integer(),
    'type': fields.String(required=True),
    'required': fields.Boolean()
})

processor_template_dto = api.model('Processor Template', {
    'template_id': fields.String(required=True),
    'name': fields.String(required=True),
    'description': fields.String(required=True),
    'icon': fields.String(required=True),
    'limit': fields.Arbitrary(),
    'input': fields.Nested(input_description_dto, allow_null=True),
    'output': fields.Nested(output_description_dto, allow_null=True),
    'parameter_editor': fields.String(required=True),
    'parameters': {'*': fields.Wildcard(fields.Nested(parameter_description_dto))},
    'processor_type': fields.String(required=True),
    'version': fields.Integer()
})


@api.route('/')
class HealthResource(Resource):

    @api.marshal_with(server_response, skip_none=True)
    def get(self):
        return ServerResponse.success(), 200
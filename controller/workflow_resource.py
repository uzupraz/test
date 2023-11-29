from flask_restx import  fields, Resource, Namespace

from .server_response import ServerResponse


api = Namespace('workflow', description='Manages workflow related operations.')
log = api.logger

connection_dto = api.model('Connection', {
    'sourceNode': fields.String(required=True),
    'targetNode': fields.String(required=True)
})

config_dto = api.model('Config', {
    'startAt': fields.String(required=True),
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
    'hasSubflow': fields.Boolean(required=True),
    'parameters': fields.Raw(required=True),
    'nodeTemplateId': fields.String(required=True),
    'subWorkflow': fields.Nested(sub_workflow_dto)
})

# Add nodes to config after node is defined
config_dto['nodes'] = fields.List(fields.Nested(node_dto))

workflow_dto = api.model('Workflow', {
    'ownerId': fields.String(required=True),
    'workflowId': fields.String(required=True),
    'config': fields.Nested(config_dto),
    'createdBy': fields.String(required=True),
    'createdByName': fields.String(required=True),
    'creationDate': fields.String(required=True),
    'groupName': fields.String(required=True),
    'name': fields.String(required=True),
    'state': fields.String(required=True),
    'workflowVersion': fields.Integer(required=True),
    'schemaVersion': fields.Integer(required=True)
})


@api.route('/')
class WorkflowResource(Resource):


    @api.expect(workflow_dto, validate=True)
    def post(self):
        """
        Create a workflow
        """
        return ServerResponse.success(), 200

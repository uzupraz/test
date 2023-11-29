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


@health_api.route('')
class HealthResource(Resource):

    @health_api.marshal_with(server_response, skip_none=True)
    def get(self):
        return ServerResponse.success()
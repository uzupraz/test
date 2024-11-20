from flask_restx import fields, Resource, Namespace
from flask import g, request
from dacite import from_dict

from controller.server_response import ServerResponse
from controller.server_stream_response import ServerStreamResponse
from controller.common_controller import server_response
from configuration import AWSConfig, AppConfig, BedrockConfig
from service import ChatService, BedrockService
from repository import ChatRepository
from enums import ServicePermissions, ServiceStatus, APIStatus
from model import User, ModelRequest
from exception import ServiceException


api = Namespace("Chatbot API", description="API for the chatbot that lists all the chat per user, get message history, create chat session and interract with the model", path="/interconnecthub/chatbot")
log = api.logger

aws_config = AWSConfig()
app_config = AppConfig()
bedrock_config = BedrockConfig()
bedrock_service = BedrockService(bedrock_config)
chat_repository = ChatRepository(app_config, aws_config)
chat_service = ChatService(chat_repository, bedrock_service)


# Models
user_chat = api.model('User chat', {
    'chat_id': fields.String(required=True),
    'title': fields.String(required=True)
})

chat_message = api.model('Chat message', {
    'prompt': fields.String(required=True),
    'response': fields.String(required=True),
    'timestamp': fields.Integer(required=True)
})

chat = api.model('Chat id', {
    'chat_id': fields.String(required=True)
})

response = api.model('AI model response', {
    'response': fields.String(required=True)
})

# Response
chats_response_dto = api.inherit('Get chats response', server_response, {
    'payload': fields.List(fields.Nested(user_chat))
})

chat_message_history_response_dto = api.inherit('Get messages for chat response', server_response, {
    'payload': fields.List(fields.Nested(chat_message))
})

chat_response_dto = api.inherit('Chat Response', server_response, {
    'payload': fields.Nested(chat)
})

model_response_dto = api.inherit('Model Response', server_response, {
    'payload': fields.Nested(response)
})

# Request
chat_request_dto = api.model('Chat Request', {
    'model_id': fields.String(required=True, description='Chat Model id to process prompt messages'),
})

prompt_request_dto = api.model('Prompt Request', {
    'prompt': fields.String(required=True)
})


@api.route('/chats')
class ChatResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc('Get chats for user')
    @api.marshal_with(chats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        response_payload = chat_service.get_chats(
            user_id=user.sub,
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
    

    @api.doc('Create new chat for user')
    @api.expect(chat_request_dto, validate=True)
    @api.marshal_with(chat_response_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        if not user.has_permission(ServicePermissions.CHATBOT_CREATE_CHAT.value):
            log.warning('User has no permission to create chat. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE.value)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to create chat')

        model_id = api.payload['model_id']

        response_payload = chat_service.save_chat_session(
            user_id=user.sub,
            owner_id=user.organization_id,
            model_id=model_id
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
    

@api.route('/chats/<string:chat_id>/messages')
class ChatMessagesResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    @api.doc('Get chat history for a specific chat of a user')
    @api.param('size', 'Number of items to retrieve', type=int, default=20)
    @api.param('last_evaluated_key', 'Pagination key for the next set of items', type=str)
    @api.marshal_with(chat_message_history_response_dto, skip_none=True)
    def get(self, chat_id):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        size = request.args.get('size', default=20, type=int) 
        last_evaluated_key = request.args.get('last_evaluated_key', default=None, type=str)
        
        response_payload = chat_service.get_message_history(
            chat_id=chat_id, 
            size=size,
            last_evaluated_key=last_evaluated_key
            )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
    

    @api.doc('Send prompt to model and save prompt and response')
    @api.expect(prompt_request_dto, skip_none=True)
    def post(self, chat_id):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        request_data = from_dict(ModelRequest, {'chat_id': chat_id, 'prompt': api.payload['prompt']})

        response_generator = chat_service.save_chat_message(request_data.chat_id, request_data.prompt)
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerStreamResponse.success(response_generator)
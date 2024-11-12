from flask_restx import fields, Resource, Namespace
from flask import g, request
from dacite import from_dict

from controller.server_response import ServerResponse
from controller.common_controller import server_response
from configuration import AWSConfig, AppConfig
from service import ChatbotService
from repository import ChatbotRepository
from enums import APIStatus
from model import User


api = Namespace("Chatbot API", description="API for the chatbot", path="/interconnecthub/chatbot")
log = api.logger

aws_config = AWSConfig()
app_config = AppConfig()
chatbot_repository = ChatbotRepository(app_config, aws_config)
chatbot_service = ChatbotService(chatbot_repository)


# Models
user_chats = api.model('Users chats', {
    'chat_id': fields.String(required=True),
    'title': fields.String(required=True),
    'timestamp': fields.Integer(required=True)
})

chat_messages = api.model('Chat message', {
    'prompt': fields.String(required=True),
    'response': fields.String(required=True),
    'timestamp': fields.Integer(required=True)
})

chat = api.model('Session chat id', {
    'chat_id': fields.String(required=True)
})

# Response
chats_response_dto = api.inherit('Get chats response', server_response, {
    'payload': fields.List(fields.Nested(user_chats))
})

message_history_response_dto = api.inherit('Get messages for chat response', server_response, {
    'payload': fields.List(fields.Nested(chat_messages))
})

chat_session_response_dto = api.inherit('Chat Response', server_response, {
    'payload': fields.Nested(chat)
})


@api.route('/chats')
class ChatbotResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc('Get chats for user')
    @api.marshal_with(chats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))
        
        response_payload = chatbot_service.get_chats(
            user_id=user.sub,
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
    

    @api.doc('Create new chat session for user')
    @api.marshal_with(chat_session_response_dto, skip_none=True)
    def post(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        response_payload = chatbot_service.save_chat_session(
            user_id=user.sub,
            owner_id=user.organization_id,
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
    

@api.route('/<string:chat_id>/message_history')
class ChatbotMessageHistoryResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    @api.doc('Get chat history for a specific chat')
    @api.param('size', 'Number of items to retrieve', type=int, default=20)
    @api.param('last_evaluated_key', 'Pagination key for the next set of items', type=str)
    @api.marshal_with(message_history_response_dto, skip_none=True)
    def get(self, chat_id):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        size = request.args.get('size', type=int)
        last_evaluated_key = request.args.get('last_evaluated_key', default=None, type=str)
        
        response_payload = chatbot_service.get_message_history(
            chat_id=chat_id, 
            size=size,
            last_evaluated_key=last_evaluated_key
            )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 200
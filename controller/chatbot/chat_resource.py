from flask_restx import fields, Resource, Namespace
from flask import g, request
from dacite import from_dict

from controller.server_response import ServerResponse
from controller.server_stream_response import ServerStreamResponse
from controller.common_controller import server_response
from configuration import AWSConfig, AppConfig, AwsBedrockConfig
from service import ChatService, BedrockService
from repository import ChatRepository
from exception import ServiceException
from enums import APIStatus, ServiceStatus
from model import User, UserPromptRequestDTO


api = Namespace("Chatbot API", description="API for the chatbot that lists all the chat per user, get message history, create chat session and interact with the model", path="/interconnecthub/chatbot")
log = api.logger

aws_config = AWSConfig()
app_config = AppConfig()
bedrock_config = AwsBedrockConfig()
bedrock_service = BedrockService(bedrock_config)
chat_repository = ChatRepository(app_config, aws_config)
chat_service = ChatService(chat_repository, bedrock_service)


# Models
user_chat = api.model('User chat', {
    'chat_id': fields.String(required=True),
    'timestamp': fields.Integer(required=True),
    'title': fields.String(required=True)
})

chat_message_history = api.model('ChatMessageHistory', {
    'messages': fields.List(fields.Nested(api.model('ChatMessage', {
        'timestamp': fields.Integer(required=True, description='Timestamp of the message'),
        'prompt': fields.String(required=True, description='User input for the chat'),
        'response': fields.String(required=True, description='Chatbot response')
    }))),
    'pagination': fields.Nested(api.model('Pagination', {
        'size': fields.Integer(required=True, description='Number of items in the current page'),
        'last_evaluated_key': fields.String(required=False, description='Pagination key for the next set of items')
    }))
})

chat = api.model('User chat id', {
    'chat_id': fields.String(required=True)
})

response = api.model('Response for prompt', {
    'response': fields.String(required=True)
})

# Response
chats_response_dto = api.inherit('The response DTO used when chats per user is requested', server_response, {
    'payload': fields.List(fields.Nested(user_chat))
})

chat_message_history_response_dto = api.inherit('The response DTO used when messaegs per chat is requested', server_response, {
    'payload': fields.List(fields.Nested(chat_message_history))
})

chat_response_dto = api.inherit('The response DTO used when a new chat is created', server_response, {
    'payload': fields.Nested(chat)
})

model_response_dto = api.inherit('The response DTO used when response for prompt is generated', server_response, {
    'payload': fields.Nested(response)
})

# Request
chat_request_dto = api.model('Save chat request payload', {
    'model_id': fields.String(required=True, description='Chat Model id to process prompt messages'),
})

prompt_request_dto = api.model('Save message request payload', {
    'prompt': fields.String(required=True),
    'system_prompt': fields.String(required=False),
    'use_history': fields.Boolean(required=False)
})


@api.route('/chats')
class ChatListResource(Resource):


    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)


    @api.doc('Get chats for user')
    @api.marshal_with(chats_response_dto, skip_none=True)
    def get(self):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        if not user.can_access_model():
            log.warning('User has no permission to access chatbot. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE.value)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to access chatbot.')

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

        model_id = api.payload['model_id']

        if not user.can_access_model(model_id, bedrock_config.model_id):
            log.warning('User has no permission to access this model. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE.value)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to create chat for this model.')

        response_payload = chat_service.save_chat_session(
            user_id=user.sub,
            owner_id=user.organization_id,
            model_id=model_id
        )
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerResponse.success(payload=response_payload), 201
    

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

        user = from_dict(User, g.get('user'))

        if not user.can_access_model():
            log.warning('User has no permission to access chat messages. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE.value)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to access chat messages.')

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
    @api.expect(prompt_request_dto, validate=True)
    def post(self, chat_id):
        log.info('Received API Request. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.START.value)

        user = from_dict(User, g.get('user'))

        if not user.can_access_model():
            log.warning('User has no permission to send message. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.FAILURE.value)
            raise ServiceException(403, ServiceStatus.FAILURE, 'User has no permission to send message.')

        # Provide default values for `system_prompt` and `use_history`
        payload = api.payload
        request_data = from_dict(
            UserPromptRequestDTO,
            {
                'user_id': user.sub,
                'chat_id': chat_id,
                'prompt': payload.get('prompt'),  # Required field
                'system_prompt': payload.get('system_prompt', ''),  # Default to empty string
                'use_history': payload.get('use_history', True),  # Default to True
            }
        )

        response_generator = chat_service.save_chat_interaction(
            request_data.user_id,
            request_data.chat_id,
            request_data.prompt,
            request_data.system_prompt,
            request_data.use_history
        )
        
        log.info('Done API Invocation. api: %s, method: %s, status: %s', request.url, request.method, APIStatus.SUCCESS.value)
        return ServerStreamResponse.generate(response_generator)


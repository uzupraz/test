import awsgi
from flask import Flask
import flask_cors

from controller import api
from configuration import AppConfig
from context import RequestContext
from utils import LogManager

# Create a Flask application instance
app = Flask(__name__)
# Disable error message inclusion in the Flask response
app.config['ERROR_INCLUDE_MESSAGE'] = False

# Enable Cross-Origin Resource Sharing (CORS) on the Flask application
flask_cors.CORS(app)

config = AppConfig()
# Configure logging with the log level from config
LogManager.configure_logging(log_level=config.log_level)

# Allow to lazy register the API on a Flask application
api.init_app(app)

def lambda_handler(event, context):
    with app.app_context():  # Set up the application context
        # Updates request ID with aws lambda request id in the request context
        RequestContext.update_request_id(context.aws_request_id)
        print(event)
        # Save the user to flask global request (g)
        @app.before_request
        def before_request():
            RequestContext.store_authenticated_user(event)
        # Generate the response using the Flask application with the provided event and context
        return awsgi.response(app, event, context)

import awsgi
import flask_cors
from flask import Flask

from utils import LogManager


def lambda_handler(event, context):
    # Create a Flask application instance
    app = Flask(__name__)

    # Disable error message inclusion in the Flask response
    app.config['ERROR_INCLUDE_MESSAGE'] = False

    # Enable Cross-Origin Resource Sharing (CORS) on the Flask application
    flask_cors.CORS(app)

    # Configure logging settings using the imported LogManager class
    LogManager.configure_logging()

    # Generate the response using the Flask application and the provided event and context
    return awsgi.response(app, event, context)
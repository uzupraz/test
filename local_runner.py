# !!! Preload environment before importing any flask components
# Please do not change order or import
from dotenv import load_dotenv
load_dotenv(override=True)

from flask import Flask
import flask_cors
import json

from controller import api
from configuration import AppConfig
from utils import LogManager
from context import RequestContext


# NOTE: 'app' is required by tests therefore has been placed outside of the if statement
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

if __name__ == '__main__':
    with app.app_context():  # Set up the application context
        @app.before_request
        def before_request():
            RequestContext.store_authenticated_user(
                {
                    "requestContext": {
                        "authorizer": {
                            "claims": {
                                "sub": "this_is_owner_id",
                                "custom:organizationId": "this_is_organization_id",
                                "custom:permissions": json.dumps(['this_is_organization_id:this_is_permission'])
                            }
                        }
                    }
                }
            )
        app.run(debug=True)
        RequestContext.update_request_id()

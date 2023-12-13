# !!! Preload environment before importing any flask components
# Please do not change order or import
from dotenv import load_dotenv
load_dotenv()

from flask import Flask

from controller import api
from configuration import AppConfig, AWSConfig
from utils import LogManager


#NOTE: 'app' is required by tests therefore has been placed outside of the if statement
app = Flask(__name__)
app.config['ERROR_INCLUDE_MESSAGE'] = False

api.init_app(app)

if __name__ == '__main__':
    # config = AppConfig.get_instance()
    # aws_config = AWSConfig.get_instance()
    LogManager.configure_logging('INFO')
    app.run(debug=False)

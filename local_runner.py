# !!! Preload environment before importing any flask components
# Please do not change order or import


from flask import Flask

from controller import api
from configuration import Config
from utils import LogManager


#NOTE: 'app' is required by tests therefore has been placed outside of the if statement
app = Flask(__name__)
app.config['ERROR_INCLUDE_MESSAGE'] = False

api.init_app(app)

if __name__ == '__main__':
    config = Config.get_instance()
    LogManager.configure_logging(config.log_level)
    app.run(debug=False)

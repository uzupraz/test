import logging

from context import RequestContext

class LogManager:

    """
    LogManager class for configuring logging settings.
    """

    @classmethod
    def configure_logging(cls, log_level:str='DEBUG') -> None:
        """
        Configure the logging settings based on the provided log level.

        Args:
            log_level (str): The log level string (e.g., 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
                            with default log level as 'DEBUG'.

        This function sets up the logging configuration for the application.
        """
        # Get the numeric value of the log level string, default to DEBUG if an invalid log level is provided
        numeric_level = getattr(logging, log_level, logging.DEBUG)

        # Create a logger and set the log level
        logger = logging.getLogger()
        logger.setLevel(numeric_level)

        # Create a custom formatter instance
        log_formatter = RequestIDFormatter(
            "%(asctime)s.%(msecs)03d %(levelname)s %(process)d [%(request_id)s] --- [%(threadName)s] %(module)s:%(funcName)s:%(lineno)d - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        # Clear the existing handlers to avoid duplicate logging
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Add a StreamHandler with the custom formatter to the logger
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_formatter)
        logger.addHandler(stream_handler)


# Create a custom formatter to include the request id in log messages
class RequestIDFormatter(logging.Formatter):
    def format(self, record):
        record.request_id = RequestContext.get_request_id()  # Set the request_id in the log record
        return super().format(record)

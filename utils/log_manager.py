import logging


class LogManager:

    @classmethod
    def configure_logging(cls, log_level: str):
        """
        Configure the logging settings based on the provided log level.

        Args:
            log_level (str): The log level string (e.g., 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').

        This function sets up the logging configuration for the application.
        """
        # Get the numeric value of the log level string, default to DEBUG if invalid log level is provided
        numeric_level = getattr(logging, log_level, logging.DEBUG)

        # Configure logging settings
        logging.basicConfig(
            level=numeric_level,  # Set the logging level to the obtained numeric value
            format="%(asctime)s.%(msecs)03d %(levelname)s %(process)d --- [%(threadName)s] %(module)s:%(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[logging.StreamHandler()]  # Log messages to the console (stdout)
        )
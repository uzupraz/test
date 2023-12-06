import os


class AppConfig:
    """
    Configuration required for retrieving the os or local environment variables
    Any environment variables to be used in the application should be set here
    """

    _instance = None


    def __init__(self, log_level:str) -> None:
        """
        Initialize the Config object with the provided attributes.

        Args:
            log_level (str): Logging level string (e.g., 'DEBUG', 'INFO', 'WARNING', etc.).
        """
        self.log_level = log_level


    @classmethod
    def _load(cls) -> 'AppConfig':
        """
        Load configuration values from environment variables.

        Returns:
            Config: Config object with loaded values.
        """
        log_level =  os.getenv('APP_LOG_LEVEL', 'DEBUG').upper()

        return cls(log_level)


    @classmethod
    def get_instance(cls, prefer=None) -> 'AppConfig':
        """
        Get the Config instance, loading values if not loaded already.

        Args:
            prefer (Config, optional): A preferred Config instance. Defaults to None.

        Returns:
            Config: The Config instance.
        """
        if not cls._instance:
            cls._instance = prefer if prefer else AppConfig._load()

        return cls._instance

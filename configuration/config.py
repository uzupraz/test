import os


class AppConfig:
    """
    Configuration required for retrieving the os or local environment variables
    Any environment variables to be used in the application should be set here
    """

    _instance = None


    def __init__(self, log_level:str, workflow_table_name:str) -> None:
        """
        Initialize the Config object with the provided attributes.

        Args:
            log_level (str): Logging level string (e.g., 'DEBUG', 'INFO', 'WARNING', etc.).
            workflow_table_name (str): The name of the DynamoDB table where the workflow data is stored.
        """
        self.log_level = log_level
        self.workflow_table_name = workflow_table_name


    @classmethod
    def _load(cls) -> 'AppConfig':
        """
        Load configuration values from environment variables.

        Returns:
            Config: Config object with loaded values.
        """
        log_level =  os.getenv('APP_LOG_LEVEL', 'DEBUG').upper()
        workflow_table_name = os.getenv('APP_WORKFLOW_TABLE_NAME')

        return cls(log_level, workflow_table_name)


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


class AWSConfig:
    """
    Configuration related to the AWS are loaded here.
    """

    _instance = None


    def __init__(self, is_local:bool, aws_region:str) -> None:
        """
        Initialize the AWSConfig object with the provided attributes.

        Args:
            is_local (bool): Whether the application is running locally or not.
            aws_region (str): The AWS region where the application is running.
        """
        self.is_local = is_local
        self.aws_region = aws_region


    @classmethod
    def _load(cls) -> 'AWSConfig':
        """
        Load configuration values from environment variables.

        Returns:
            AWSConfig: AWSConfig object with loaded values.
        """
        is_local = os.getenv('AWS_IS_LOCAL', 'False').lower() == 'true'
        aws_region = os.getenv('AWS_REGION')

        return cls(is_local, aws_region)


    @classmethod
    def get_instance(cls, prefer=None) -> 'AWSConfig':
        """
        Get the AWSConfig instance, loading values if not loaded already.

        Args:
            prefer (AWSConfig, optional): A preferred AWSConfig instance. Defaults to None.

        Returns:
            AWSConfig: The AWSConfig instance.
        """
        if not cls._instance:
            cls._instance = prefer if prefer else AWSConfig._load()

        return cls._instance

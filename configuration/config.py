import os
import dataclasses


class Singleton(type):
    """
    Singleton is a type of metaclass that ensures a class has only one instance.
    If an instance of the class already exists, it returns that instance.
    If not, it creates a new instance and stores it for future reference.

    """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        """
        This method is called when the class is "called" (i.e., instantiated).
        It first checks if an instance of the class already exists.
        If it does, it returns the existing instance.
        If not, it creates a new instance, stores it in the _instances dictionary, and returns it.

        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclasses.dataclass(init=False)
class AppConfig(metaclass=Singleton):
    """
    Configuration required for retrieving the os or local environment variables
    Any environment variables to be used in the application should be set here
    """
    log_level: str = os.getenv('APP_LOG_LEVEL', 'DEBUG').upper()
    workflow_table_name: str = os.getenv('APP_WORKFLOW_TABLE_NAME')


@dataclasses.dataclass(init=False)
class AWSConfig(metaclass=Singleton):
    """
    Configuration related to the AWS are loaded here.
    """
    is_local: bool = os.getenv('AWS_IS_LOCAL', 'False').lower() == 'true'
    aws_region: str = os.getenv('AWS_REGION')

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

    @classmethod
    def clear_instance(cls, class_type):
        if class_type in cls._instances:
            cls._instances.pop(class_type)
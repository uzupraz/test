from typing import List, Optional

from model import DataFormat
from repository import DataFormatsRepository
from controller import common_controller as common_ctrl
from utils import Singleton


log = common_ctrl.log


class DataFormatsService(metaclass=Singleton):


    def __init__(self, data_formats_repository: DataFormatsRepository) -> None:
        self.data_formats_repository = data_formats_repository


    def list_all_data_formats(self) -> List[DataFormat]:
        """
        Retrieve all data formats using the data formats repository.

        Returns:
            List[DataFormat]: A list of DataFormat objects retrieved from the repository.
        """
        return self.data_formats_repository.list_all_data_formats()
    

    def get_data_format(self, format: str) -> Optional[DataFormat]:
        """
        Retrieve data format using the data format repository.

        Args:
            format (str): The name of the data format.

        Returns:
            Optional[DataFormat]: A DataFormat object retrieved from the repository.
        """
        return self.data_formats_repository.get_data_format(format)

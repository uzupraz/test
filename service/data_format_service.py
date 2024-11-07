from typing import List

from model import DataFormat
from repository import DataFormatRepository
from controller import common_controller as common_ctrl
from utils import Singleton


log = common_ctrl.log


class DataFormatService(metaclass=Singleton):


    def __init__(self, data_format_repository: DataFormatRepository) -> None:
        self.data_format_repository = data_format_repository


    def get_data_formats(self) -> List[DataFormat]:
        return self.data_format_repository.get_data_formats()

from typing import List

from model import ProcessorTemplate
from repository import ProcessorTemplateRepo
from controller import common_controller as common_ctrl
from utils import Singleton


log = common_ctrl.log


class ProcessorTemplateService(metaclass=Singleton):


    _instance = None


    def __init__(self, repo: ProcessorTemplateRepo) -> None:
        self.repo = repo


    def get_all_templates(self) -> List[ProcessorTemplate]:
        """
        List all the Processor templates available.

        Returns:
            A list of processor templates, empty list if not available
        """
        return self.repo.get_all_templates()

from controller import common_controller as common_ctrl
from utils import Singleton
from model import DataTable
from repository import CustomerTableInfoRepository

log = common_ctrl.log

class DataTableService(metaclass=Singleton):


    def __init__(self, customer_table_info_repository:CustomerTableInfoRepository) -> None:
        """
        Initializes the DataTableService with the CustomerTableInfoRepository.

        Args:
            customer_table_info_repository (CustomerTableInfoRepository): The repository instance to access customer table information.
        """
        self.customer_table_info_repository = customer_table_info_repository


    def list_tables(self, owner_id:str) -> list[DataTable]:
        """
        Retrieves the list of DynamoDB tables that belong to the specified owner.

        Args:
            owner_id (str): The id of the owner, for whom the list of tables belong to.

        Returns:
            List[DataTable]: A list of DataTable objects containing table details.
        """
        log.info('Retrieving all tables. ownerId: %s', owner_id)
        table_details = self.customer_table_info_repository.get_tables_for_owner(owner_id)
        data_tables = []

        for table_detail in table_details:
            table_size = self.customer_table_info_repository.get_table_size(table_detail['original_table_name'])
            table_info = DataTable(
                name=table_detail['table_name'],
                id=table_detail['table_id'],
                size=table_size
            )
            data_tables.append(table_info)
        return data_tables

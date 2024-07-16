from controller import common_controller as common_ctrl
from utils import Singleton
from model import DataTable
from exception import ServiceException
from enums import ServiceStatus
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
        log.info('Retrieving all tables. owner_id: %s', owner_id)
        if not owner_id:
            log.exception('Failed to retrieve all tables. owner_id: %s', owner_id)
            raise ServiceException(400, ServiceStatus.FAILURE.value, 'owner id cannot be null or empty')
        tables_response = self.customer_table_info_repository.get_tables_for_owner(owner_id)
        data_tables = []

        for table in tables_response:
            table_details_response = self.customer_table_info_repository.get_table_details(table['original_table_name'])
            data_tables.append(DataTable(
                name=table['table_name'],
                id=table['table_id'],
                size=table_details_response['Table'] ['TableSizeBytes'] / 1024
            ))
        return data_tables

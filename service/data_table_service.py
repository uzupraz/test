from controller import common_controller as common_ctrl
from utils import Singleton
from model import ListTableResponse, UpdateTableRequest
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


    def list_tables(self, owner_id:str) -> list[ListTableResponse]:
        """
        Retrieves the list of DynamoDB tables that belong to the specified owner.

        Args:
            owner_id (str): The id of the owner, for whom the list of tables belong to.

        Returns:
            List[ListTableResponse]: A list of tables containing table details.
        """
        log.info('Retrieving all tables. owner_id: %s', owner_id)
        tables_response = self.customer_table_info_repository.get_tables_for_owner(owner_id)
        owner_tables  = []

        for table in tables_response:
            table_details_response = self.customer_table_info_repository.get_table_details(table.original_table_name)
            owner_tables .append(ListTableResponse(
                name=table.table_name,
                id=table.table_id,
                size=table_details_response['Table'] ['TableSizeBytes'] / 1024
            ))
        return owner_tables


    def update_table(self, owner_id:str, table_id:str, update_data:UpdateTableRequest) -> None:
        """
        Updates the fields of a customer's table.

        Args:
            owner_id (str): The owner of the table.
            table_id (str): The ID of the table.
            update_data (str): The data to update in the customer's table.
        """
        log.debug('Updating customer table. update_data: %s', update_data)
        self.customer_table_info_repository.update_table(owner_id, table_id, update_data)

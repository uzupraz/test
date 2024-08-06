from dacite import from_dict
from dataclasses import asdict

from controller import common_controller as common_ctrl
from utils import Singleton
from model import ListTableResponse, UpdateTableRequest, TableDetails
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
        log.info('Retrieving customer tables. owner_id: %s', owner_id)
        tables = self.customer_table_info_repository.get_tables_for_owner(owner_id)
        owner_tables  = []

        for table in tables:
            dynamoDB_table_details = self.customer_table_info_repository.get_dynamoDB_table_details(table.original_table_name)
            owner_tables .append(ListTableResponse(
                name=table.table_name,
                id=table.table_id,
                size=dynamoDB_table_details.size
            ))
        return owner_tables


    def update_description(self, owner_id:str, table_id:str, update_table_request:UpdateTableRequest) -> TableDetails:
        """
        Updates the description field of a customer's table.

        Args:
            owner_id (str): The owner of the table.
            table_id (str): The ID of the table.
            update_table_request (UpdateTableRequest): The data to update in the customer's table.

        Returns:
            TableDetails: The customer table details after update.
        """
        log.debug('Updating customer table. update_data: %s', update_table_request)
        # Check if the item exists
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        # set the fields to update in an existing item
        customer_table_info.description = update_table_request.description
        updated_customer_table_info = self.customer_table_info_repository.update_description(customer_table_info)
        dynamoDB_table_details = self.customer_table_info_repository.get_dynamoDB_table_details(updated_customer_table_info.original_table_name)
        # Convert updated customer table info to UpdateTableResponse
        updated_table = from_dict(TableDetails, asdict(updated_customer_table_info))
        for index in updated_table.indices:
            # table size equals index size
            index.size = dynamoDB_table_details.size
        return updated_table


    def get_table_details(self, owner_id:str, table_id:str) -> TableDetails:
        """
        Retrieve the details of a specific table by its owner_id and table_id.

        Args:
            owner_id (str): The ID of the owner of the table.
            table_id (str): The ID of the table.

        Returns:
            TableDetails: An object containing detailed information about the table.
        """
        log.info('Retrieving table details. owner_id: %s, table_id: %s', owner_id, table_id)
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        table_details = from_dict(TableDetails, asdict(customer_table_info))
        dynamoDB_table_details = self.customer_table_info_repository.get_dynamoDB_table_details(customer_table_info.original_table_name)
        for index in table_details.indices:
            # table size equals index size
            index.size = dynamoDB_table_details.size
        return table_details

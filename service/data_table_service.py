import json
import base64
from dacite import from_dict
from dataclasses import asdict

from controller import common_controller as common_ctrl
from utils import Singleton
from model import ListTableResponse, UpdateTableRequest, UpdateTableResponse, CustomerTableItem, CustomerTableItemPagination
from repository import CustomerTableInfoRepository, CustomerTableRepository

log = common_ctrl.log

class DataTableService(metaclass=Singleton):


    def __init__(self, customer_table_info_repository:CustomerTableInfoRepository, customer_table_repository:CustomerTableRepository) -> None:
        """
        Initializes the DataTableService with the CustomerTableInfoRepository.

        Args:
            customer_table_info_repository (CustomerTableInfoRepository): The repository instance to access customer table information.
            customer_table_repository (CustomerTableRepository): The repository instance to access tables information.
        """
        self.customer_table_info_repository = customer_table_info_repository
        self.customer_table_repository = customer_table_repository


    def list_tables(self, owner_id:str) -> list[ListTableResponse]:
        """
        Retrieves the list of DynamoDB tables that belong to the specified owner.

        Args:
            owner_id (str): The id of the owner, for whom the list of tables belong to.

        Returns:
            List[ListTableResponse]: A list of tables containing table details.
        """
        log.info('Retrieving customer tables. owner_id: %s', owner_id)
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


    def update_table(self, owner_id:str, table_id:str, update_table_request:UpdateTableRequest) -> UpdateTableResponse:
        """
        Updates the fields of a customer's table.

        Args:
            owner_id (str): The owner of the table.
            table_id (str): The ID of the table.
            update_table_request (UpdateTableRequest): The data to update in the customer's table.

        Returns:
            UpdateTableResponse: The customer table details after update.
        """
        log.debug('Updating customer table. update_data: %s', update_table_request)
        # Check if the item exists
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        # set the fields to update in an existing item
        customer_table_info.description = update_table_request.description
        updated_customer_table_info = self.customer_table_info_repository.update_table(customer_table_info)
        # Convert updated customer table info to UpdateTableResponse
        update_table_response = from_dict(UpdateTableResponse, asdict(updated_customer_table_info))
        return update_table_response
    

    def get_table_items(self, owner_id:str, table_id:str, size:int, last_evaluated_key:str|None=None) -> CustomerTableItem:
        """
        Get the items of the table with provided table_id.

        Args:
            owner_id (str): The owner of the table.
            table_id (str): The ID of the table.
            size (int): Size of rows to fetch.
            last_evaluated_key (str|None): Last evaluated key of previous request.

        Returns:
            CustomerTableContent: The customer table content in paginated form.
        """
        log.info('Fetching table items. owner_id: %s, table_id: %s', owner_id, table_id)
        # Check if the item exists
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        # Decoding last evaluated_key from base64
        if last_evaluated_key is not None:
            last_evaluated_key = json.loads(base64.b64decode(last_evaluated_key).decode('utf-8'))

        # querying database with exclusive start key
        items, last_evaluated_key = self.customer_table_repository.get_table_items(
            table_name=customer_table_info.original_table_name, 
            limit=size,
            exclusive_start_key=last_evaluated_key
        )
        # Encoding last evaluated_key into base64
        encoded_last_evaluated_key = None
        if last_evaluated_key is not None and isinstance(last_evaluated_key, dict):
            key = json.dumps(last_evaluated_key).encode('utf-8')
            encoded_last_evaluated_key = base64.b64encode(key).decode('utf-8')


        return CustomerTableItem(
            items=items,
            pagination=CustomerTableItemPagination(
                size=size,
                last_evaluated_key=encoded_last_evaluated_key
            )
        )

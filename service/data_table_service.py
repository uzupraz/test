import json
import base64

from controller import common_controller as common_ctrl
from utils import Singleton
from model import ListTableResponse, UpdateTableRequest, CustomerTableInfo, CustomerTableItem, CustomerTableItemPagination
from repository import CustomerTableInfoRepository, CustomerTableRepository

log = common_ctrl.log
ENCODING_FORMAT = 'utf-8'

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
        tables = self.customer_table_info_repository.get_tables_for_owner(owner_id)
        owner_tables  = []

        for table in tables:
            table_size = self.customer_table_info_repository.get_table_size(table.original_table_name)
            owner_tables.append(ListTableResponse(
                name=table.table_name,
                id=table.table_id,
                size=table_size
            ))
        return owner_tables


    def update_description(self, owner_id:str, table_id:str, update_table_request:UpdateTableRequest) -> CustomerTableInfo:
        """
        Updates the description field of a customer's table.

        Args:
            owner_id (str): The owner of the table.
            table_id (str): The ID of the table.
            update_table_request (UpdateTableRequest): The data to update in the customer's table.

        Returns:
            CustomerTableInfo: The customer table info after update.
        """
        log.debug('Updating customer table. update_data: %s', update_table_request)
        # Check if the item exists
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        # set the fields to update in an existing item
        customer_table_info.description = update_table_request.description
        updated_customer_table_info = self.customer_table_info_repository.update_description(customer_table_info)

        table_size = self.customer_table_info_repository.get_table_size(updated_customer_table_info.original_table_name)
        for index in updated_customer_table_info.indexes:
            # table size equals index size
            index.size = table_size
        return updated_customer_table_info


    def get_table_info(self, owner_id:str, table_id:str) -> CustomerTableInfo:
        """
        Retrieve the info of a specific table by its owner_id and table_id.

        Args:
            owner_id (str): The ID of the owner of the table.
            table_id (str): The ID of the table.

        Returns:
            CustomerTableInfo: An object containing detailed information about the customer table.
        """
        log.info('Retrieving customer table info. owner_id: %s, table_id: %s', owner_id, table_id)
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        table_size = self.customer_table_info_repository.get_table_size(customer_table_info.original_table_name)
        for index in customer_table_info.indexes:
            # table size equals index size
            index.size = table_size
        return customer_table_info


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
            last_evaluated_key = json.loads(base64.b64decode(last_evaluated_key).decode(ENCODING_FORMAT))

        # querying database with exclusive start key
        items, last_evaluated_key = self.customer_table_repository.get_table_items(
            table_name=customer_table_info.original_table_name, 
            limit=size,
            exclusive_start_key=last_evaluated_key
        )
        # Encoding last evaluated_key into base64
        encoded_last_evaluated_key = None
        if last_evaluated_key is not None and isinstance(last_evaluated_key, dict):
            key = json.dumps(last_evaluated_key).encode(ENCODING_FORMAT)
            encoded_last_evaluated_key = base64.b64encode(key).decode(ENCODING_FORMAT)


        return CustomerTableItem(
            items=items,
            pagination=CustomerTableItemPagination(
                size=size,
                last_evaluated_key=encoded_last_evaluated_key
            )
        )

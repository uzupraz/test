from dacite import from_dict
from dataclasses import asdict

from controller import common_controller as common_ctrl
from utils import Singleton
from model import ListTableResponse, UpdateTableRequest, CustomerTableInfo, BackupJob
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


    def get_table_backup_jobs(self, owner_id:str, table_id:str) -> BackupJob:
        """
        Retrieve the backup jobs of a specific table by its owner_id and table_id.

        Args:
            owner_id (str): The ID of the owner of the table.
            table_id (str): The ID of the table.

        Returns:
            list[BackupJob]: The backup jobs of dynamoDB table.

        Raises:
            ServiceException: If there is an error, retrieving the table item or backup jobs.
        """
        log.info('Retrieving customer table backup jobs. owner_id: %s, table_id: %s', owner_id, table_id)
        customer_table_info = self.customer_table_info_repository.get_table_item(owner_id, table_id)
        backup_details = self.customer_table_info_repository.get_table_backup_jobs(customer_table_info.original_table_name, customer_table_info.table_arn)
        return backup_details

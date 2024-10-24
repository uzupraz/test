import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from repository import CsaMachinesRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from model import MachineInfo, Module
from utils import Singleton


class TestCsaMachinesRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/csa/'


    def setUp(self) -> None:
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_table = Mock()

        Singleton.clear_instance(CsaMachinesRepository)
        with patch('repository.csa.csa_machines_repository.CsaMachinesRepository._CsaMachinesRepository__configure_dynamodb') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            mock_configure_resource.return_value = self.mock_dynamodb_table
            self.csa_machine_repo = CsaMachinesRepository(self.app_config, self.aws_config)


    def tearDown(self) -> None:
        self.csa_machine_repo = None
        self.mock_configure_resource = None


    def test_get_csa_machine_info_success_case(self):
        """
        Test case for successfully retrieving CSA machine information.

        Case: Valid owner ID and machine ID are provided, and the DynamoDB returns the expected item.
        Expected Result: The method returns a MachineInfo object with the correct owner ID, 
        machine ID, platform, and module information.
        """
        # Mock DynamoDB response
        item = TestUtils.get_file_content(self.test_resource_path + 'csa_machines_updater_items.json')
        self.mock_dynamodb_table.get_item.return_value = {"Item": item} 

        # Call method
        machine_info = self.csa_machine_repo.get_csa_machine_info("owner123", "machine123")

        # Assertions
        self.assertIsInstance(machine_info, MachineInfo)  
        self.assertEqual(machine_info.owner_id, "owner123")
        self.assertEqual(machine_info.machine_id, "machine123")
        self.assertEqual(machine_info.platform, "platform")
        self.assertEqual(machine_info.modules[0].module_name, "module_name")
        self.assertEqual(machine_info.modules[0].version, "1.1.0")

        # Verify the mock was called with the expected arguments
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={"owner_id": "owner123", "machine_id": "machine123"})


    def test_get_csa_machine_info_raises_service_exception_on_client_error(self):
        """
        Test case for handling DynamoDB ClientError when retrieving CSA machine information.

        Case: A ClientError is raised when attempting to get item from DynamoDB.
        Expected Result: The method raises a ServiceException indicating failure to retrieve 
        owner's machine info with the correct status code and message.
        """
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.get_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "query"
        )

        # Call the method under test
        with self.assertRaises(ServiceException) as e:
            self.csa_machine_repo.get_csa_machine_info("owner123", "machine123")

        # Assertions
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Could not retrieve owner's machine info")


    def test_get_csa_machine_info_no_result(self):
        """
        Test case for handling the scenario when no result is returned from DynamoDB.

        Case: The DynamoDB returns an empty response for the provided owner ID and machine ID.
        Expected Result: The method raises a ServiceException indicating that machine info 
        does not exist.
        """
        # Mock DynamoDB to return an empty response
        self.mock_dynamodb_table.get_item.return_value = {}

        # Call method 
        with self.assertRaises(ServiceException) as e:
            self.csa_machine_repo.get_csa_machine_info("owner123", "machine123")

        # Assertions
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Machine info does not exists")
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={"owner_id": "owner123", "machine_id": "machine123"})


    def test_get_csa_machine_info_invalid_keys(self):
        """
        Test case for handling invalid owner ID and machine ID.

        Case: Invalid owner ID and machine ID are provided, resulting in no item found.
        Expected Result: The method raises a ServiceException indicating that machine info 
        does not exist with the provided keys.
        """
        #Mock Dynamodb response
        self.mock_dynamodb_table.get_item.return_value = {}

        #Method under test
        with self.assertRaises(ServiceException) as e:
            self.csa_machine_repo.get_csa_machine_info("invalid_owner", "invalid_machine")

        #Assertions
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Machine info does not exists")
        self.mock_dynamodb_table.get_item.assert_called_once_with(Key={"owner_id": "invalid_owner", "machine_id": "invalid_machine"})


    def test_update_modules_success(self):
        """
        Test case for successfully updating modules in DynamoDB.

        Case: Valid owner ID, machine ID, and a list of modules are provided for updating.
        Expected Result: The method calls update_item on DynamoDB with the correct parameters 
        and updates the modules as expected.
        """
        # Mock successful update
        self.mock_dynamodb_table.update_item.return_value = {}

        # Call method
        modules = [Module(module_name="module_name", version="1.0.0")]
        self.csa_machine_repo.update_modules("owner123", "machine123", modules)

        # Assertions
        self.mock_dynamodb_table.update_item.assert_called_once_with(
            Key={'owner_id': 'owner123', 'machine_id': 'machine123'},
            UpdateExpression="SET modules = :updateList",
            ExpressionAttributeValues={':updateList': modules}
        )
        self.mock_dynamodb_table.update_item.assert_called_once()


    def test_update_modules_empty_list(self):
        """
        Test case for handling an empty list of modules during update.

        Case: An empty list is passed as the modules parameter for updating.
        Expected Result: The method does not call update_item on DynamoDB.
        """
        # Call method with empty modules list
        self.csa_machine_repo.update_modules("owner123", "machine123", [])

        # Assert that update_item was never called
        self.mock_dynamodb_table.update_item.assert_not_called()


    def test_update_modules_throws_client_exception(self):
        """
        Test case for handling DynamoDB ClientError when updating modules.

        Case: A ClientError is raised during the update_item operation.
        Expected Result: The method raises a ServiceException indicating failure to update 
        modules with the correct status code and message.
        """
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.update_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "update_item"
        )

        # Test exception handling
        modules = [Module(module_name="module_name", version="1.0.0")]
        with self.assertRaises(ServiceException) as e:
            self.csa_machine_repo.update_modules("owner123", "machine123", modules)

        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Failed to update modules")
        self.mock_dynamodb_table.update_item.assert_called_once()


    def test_update_modules_missing_key(self):
        """
        Test case for handling missing owner ID and machine ID during module update.

        Case: Empty strings are provided for owner ID and machine ID, resulting in a 
        ClientError during update.
        Expected Result: The method raises a ServiceException indicating failure to update 
        modules with the correct status code and message.
        """
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.update_item.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "update_item"
        )

        # Test exception handling
        modules = [Module(module_name="module_name", version="1.0.0")]
        with self.assertRaises(ServiceException) as e:
            self.csa_machine_repo.update_modules("", "", modules)

        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Failed to update modules")
        self.mock_dynamodb_table.update_item.assert_called_once()
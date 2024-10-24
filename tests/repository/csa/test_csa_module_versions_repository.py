import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from repository import CsaModuleVersionsRepository
from tests.test_utils import TestUtils
from exception import ServiceException
from model import ModuleInfo
from utils import Singleton


class TestCsaModuleVersionsRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/csa/'


    def setUp(self) -> None:
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_table = Mock()

        Singleton.clear_instance(CsaModuleVersionsRepository)
        with patch('repository.csa.csa_module_versions_repository.CsaModuleVersionsRepository._CsaModuleVersionsRepository__configure_dynamodb') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            mock_configure_resource.return_value = self.mock_dynamodb_table
            self.csa_module_versions_repo = CsaModuleVersionsRepository(self.app_config, self.aws_config)

        
    def tearDown(self) -> None:
        self.csa_module_versions_repo = None
        self.mock_configure_resource = None


    def test_get_csa_module_versions_success_case(self):
        """
        Test case for successfully retrieving CSA module versions.

        Case: Valid module name is provided, and the DynamoDB returns the expected items.
        Expected Result: The method returns a list of ModuleInfo objects with the correct 
        module name, version, and checksum.
        """
        # Mock DynamoDB response
        items = [TestUtils.get_file_content(self.test_resource_path + 'module_version_updater_items.json')]
        self.mock_dynamodb_table.query.return_value = {"Items": items}

        # Call method
        module_info = self.csa_module_versions_repo.get_csa_module_versions("module_name")

        # Assertions
        self.assertEqual(len(module_info), 1)
        self.assertTrue(isinstance(module_info[0], ModuleInfo))
        self.assertEqual(module_info[0].module_name, "module_name")
        self.assertEqual(module_info[0].version, "1.0.0")
        self.assertEqual(module_info[0].checksum, "checksum123")

        self.mock_dynamodb_table.query.assert_called_once_with(KeyConditionExpression=Key('module_name').eq('module_name'))


    def test_get_csa_module_versions_raises_service_exception_on_client_error(self):
        """
        Test case for handling DynamoDB ClientError when retrieving CSA module versions.

        Case: A ClientError is raised when attempting to query DynamoDB.
        Expected Result: The method raises a ServiceException indicating failure to 
        retrieve modules with the correct status code and message.
        """
        # Mock DynamoDB ClientError
        self.mock_dynamodb_table.query.side_effect = ClientError(
            {"Error": {"Message": "Test Error"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "query"
        )

        # Test exception handling
        with self.assertRaises(ServiceException) as context:
            self.csa_module_versions_repo.get_csa_module_versions("module_name")

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.message, "Could not retrieve modules")


    def test_get_csa_module_versions_multiple_item(self):
        """
        Test case for retrieving multiple CSA module versions.

        Case: Valid module name is provided, and the DynamoDB returns multiple items.
        Expected Result: The method returns a list of ModuleInfo objects for each version,
        correctly populated with module name, version, and checksum.
        """
        # Mock DynamoDB response with multiple versions
        items = [
            {
                "module_name": "module_name",
                "version": "1.0.0",
                "checksum": "checksum123"
            },
            {
                "module_name": "module_name",
                "version": "1.1.0",
                "checksum": "checksum456"
            }
        ]
        self.mock_dynamodb_table.query.return_value = {"Items": items}

        # Call method
        module_info = self.csa_module_versions_repo.get_csa_module_versions("module_name")

        # Assertions
        self.assertEqual(len(module_info), 2)
        self.assertTrue(isinstance(module_info[0], ModuleInfo))
        self.assertTrue(isinstance(module_info[1], ModuleInfo))

        # Check the first version details
        self.assertEqual(module_info[0].module_name, "module_name")
        self.assertEqual(module_info[0].version, "1.0.0")
        self.assertEqual(module_info[0].checksum, "checksum123")

        # Check the second version details
        self.assertEqual(module_info[1].module_name, "module_name")
        self.assertEqual(module_info[1].version, "1.1.0")
        self.assertEqual(module_info[1].checksum, "checksum456")

        self.mock_dynamodb_table.query.assert_called_once_with(KeyConditionExpression=Key('module_name').eq('module_name'))


    def test_get_csa_module_versions_no_result(self):
        """
        Test case for handling the scenario when no module versions are returned.

        Case: The DynamoDB returns an empty list for the provided module name.
        Expected Result: The method raises a ServiceException indicating that modules 
        do not exist.
        """
        # Mock DynamoDB response with no items
        self.mock_dynamodb_table.query.return_value = {"Items": []}

        # Call method
        with self.assertRaises(ServiceException) as e:
            self.csa_module_versions_repo.get_csa_module_versions("module_name")

        # Assertions
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Modules do not exist")

        self.mock_dynamodb_table.query.assert_called_once_with(KeyConditionExpression=Key('module_name').eq('module_name'))


    def test_get_csa_module_version_invalid_key(self):
        """
        Test case for handling invalid module name during module version retrieval.

        Case: An invalid module name is provided, resulting in no items found.
        Expected Result: The method raises a ServiceException indicating that modules 
        do not exist.
        """
        # Mock Dynamodb response with no item
        self.mock_dynamodb_table.query.return_value = {"Items": []}

        # Call method under test
        with self.assertRaises(ServiceException) as e:
            self.csa_module_versions_repo.get_csa_module_versions("invalid_module_name")

        # Assertions 
        self.assertEqual(e.exception.status_code, 400)
        self.assertEqual(e.exception.message, "Modules do not exist")

        self.mock_dynamodb_table.query.assert_called_once_with(KeyConditionExpression=Key('module_name').eq('invalid_module_name'))

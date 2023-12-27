import unittest
import dataclasses
from unittest import mock
from botocore.exceptions import ClientError

from configuration import AWSConfig, AppConfig
from repository import WorkflowRepository
from ..test_utils import TestUtils
from model import Workflow
from exception import ServiceException


@unittest.skip('Failing Tests due to bad configuration')
class TestWorkflowRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/workflow_converter/'


    def setUp(self) -> None:
        self.aws_config = AWSConfig()
        self.app_config = AppConfig()
        self.workflow_repository = WorkflowRepository.get_instance(self.app_config, self.aws_config)
        self.workflow_repository.workflow_table = mock.MagicMock()


    def tearDown(self) -> None:
        self.app_config = None
        self.aws_config = None
        self.workflow_repository = None
        WorkflowRepository._instance = None
        AWSConfig._instance = None
        AppConfig._instance = None


    def test_save_happy_case_should_successfully_save_the_workflow(self):
        """
        This function is used to test the happy case scenario where the workflow is successfully saved. It asserts that the saved workflow is equal to the original workflow and that the `put_item` method of the `workflow_table` attribute is called once with the JSON representation of the workflow.
        """
        input_file_path = self.test_resource_path + 'custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        self.workflow_repository.workflow_table.put_item = mock.MagicMock()

        actual_workflow = self.workflow_repository.save(workflow)

        self.assertEqual(workflow, actual_workflow)
        self.workflow_repository.workflow_table.put_item.assert_called_once_with(Item=dataclasses.asdict(workflow))


    def test_save_when_client_error_is_thrown_by_dynamodb_should_raise_service_exception(self):
        """
        Test if the function raises a ServiceException when a ClientError is thrown by DynamoDB.
        """
        input_file_path = self.test_resource_path + 'custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        self.workflow_repository.workflow_table.put_item = mock.MagicMock()
        self.workflow_repository.workflow_table.put_item.side_effect = ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'put_item')

        with self.assertRaises(ServiceException):
            self.workflow_repository.save(workflow)

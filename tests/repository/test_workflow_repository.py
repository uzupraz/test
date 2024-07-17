import unittest
import dataclasses
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key,Attr

from repository import WorkflowRepository
from tests import TestUtils
from model import Workflow
from exception import ServiceException
from utils import Singleton


class TestWorkflowRepository(unittest.TestCase):


    test_resource_path = '/tests/resources/workflow_converter/'


    def setUp(self) -> None:
        self.mock_table = Mock()
        self.app_config = Mock()
        self.aws_config = Mock()
        Singleton.clear_instance(WorkflowRepository)
        with patch('repository.workflow_repository.WorkflowRepository._WorkflowRepository__configure_table') as mock_configure_table:
            self.mock_configure_table = mock_configure_table
            mock_configure_table.return_value = self.mock_table
            self.workflow_repository = WorkflowRepository(self.app_config, self.aws_config)


    def tearDown(self) -> None:
        self.app_config = None
        self.aws_config = None
        self.workflow_repository = None


    @unittest.skip
    def test_save_happy_case_should_successfully_save_the_workflow(self):
        """
        This function is used to test the happy case scenario where the workflow is successfully saved. It asserts that the saved workflow is equal to the original workflow and that the `put_item` method of the `workflow_table` attribute is called once with the JSON representation of the workflow.
        """
        input_file_path = self.test_resource_path + 'custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        self.workflow_repository.workflow_table.put_item = MagicMock()

        actual_workflow = self.workflow_repository.save(workflow)

        self.assertEqual(workflow, actual_workflow)
        self.workflow_repository.workflow_table.put_item.assert_called_once_with(Item=dataclasses.asdict(workflow))

    @unittest.skip
    def test_save_when_client_error_is_thrown_by_dynamodb_should_raise_service_exception(self):
        """
        Test if the function raises a ServiceException when a ClientError is thrown by DynamoDB.
        """
        input_file_path = self.test_resource_path + 'custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        self.workflow_repository.workflow_table.put_item = MagicMock()
        self.workflow_repository.workflow_table.put_item.side_effect = ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'put_item')

        with self.assertRaises(ServiceException):
            self.workflow_repository.save(workflow)


    def test_count_active_workflows_happy_case_should_return_correct_count(self):
        """
        Test if the function correctly returns the count of active workflows for a given owner.
        """
        owner_id = "owner123"
        expected_count = 5
        self.workflow_repository.workflow_table.query = MagicMock(return_value={'Count': expected_count})

        actual_count = self.workflow_repository.count_active_workflows(owner_id)

        self.assertEqual(expected_count, actual_count)
        self.workflow_repository.workflow_table.query.assert_called_once_with(
            KeyConditionExpression=Key('ownerId').eq(owner_id),
            FilterExpression=Attr('state').eq('ACTIVE'),
        )


    def test_count_active_workflows_when_client_error_is_thrown_should_raise_service_exception(self):
        """
        Test if the function raises a ServiceException when a ClientError is thrown by DynamoDB during the count of active workflows.
        """
        owner_id = "owner123"
        self.workflow_repository.workflow_table.query = MagicMock()
        self.workflow_repository.workflow_table.query.side_effect = ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query')

        with self.assertRaises(ServiceException):
            self.workflow_repository.count_active_workflows(owner_id)


    def test_get_data_studio_workflows_happy_case_should_return_correct_list(self):
        """
        Test if the function correctly returns the list of datastudio workflows for a given owner.
        """
        owner_id = "test_owner_id"
        
        mock_response_path = '/tests/resources/workflows/get_data_studio_workflows_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        
        self.workflow_repository.workflow_table.query = MagicMock(return_value={"Items": mock_response_items})

        actual_result = self.workflow_repository.get_data_studio_workflows(owner_id)

        self.assertEqual(mock_response_items, actual_result)
        self.workflow_repository.workflow_table.query.assert_called_once_with(
            KeyConditionExpression=Key('ownerId').eq(owner_id),
            FilterExpression=Attr('mapping_id').exists() & Attr('mapping_id').ne(None)
        )
    

    def test_get_data_studio_workflows_should_return_empty_list_when_workflows_with_mapping_id_is_not_present(self):
        """
        Test if the function correctly returns an empty list when there are no workflows with mapping_id present.
        """
        owner_id = "test_owner_id"

        mock_response_path = '/tests/resources/workflows/get_data_studio_workflows_without_mapping_id.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        self.workflow_repository.workflow_table.query = MagicMock(return_value={"Items": mock_response_items})

        actual_result = self.workflow_repository.get_data_studio_workflows(owner_id)

        self.assertEqual(mock_response_items, actual_result)
        self.workflow_repository.workflow_table.query.assert_called_once_with(
            KeyConditionExpression=Key('ownerId').eq(owner_id),
            FilterExpression=Attr('mapping_id').exists() & Attr('mapping_id').ne(None)
        )


    def test_get_data_studio_workflows_when_client_error_is_thrown_should_raise_service_exception(self):
        """
        Test if the function raises a ServiceException when a ClientError is thrown by DynamoDB.
        """
        owner_id = "owner123"
        self.workflow_repository.workflow_table.query = MagicMock()
        self.workflow_repository.workflow_table.query.side_effect = ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'query')

        with self.assertRaises(ServiceException):
            self.workflow_repository.get_data_studio_workflows(owner_id)
        
        self.workflow_repository.workflow_table.query.assert_called_once_with(
            KeyConditionExpression=Key('ownerId').eq(owner_id),
            FilterExpression=Attr('mapping_id').exists() & Attr('mapping_id').ne(None)
        )
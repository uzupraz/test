import unittest
from unittest.mock import MagicMock

from enums import ServiceStatus
from exception import ServiceException
from model import Workflow
from service import DataStudioService
from tests import TestUtils


class TestDataStudioService(unittest.TestCase):


    def setUp(self) -> None:
        self.workflow_repository = MagicMock()
        self.data_studio_service = DataStudioService(self.workflow_repository)


    def tearDown(self) -> None:
        self.workflow_repository = None
        self.dashboard_service = None


    def test_get_workflows_should_return_list_of_workflows(self):
        """
        Test if the function correctly returns the list of workflows for a given owner.
        """
        owner_id = "test_owner_id"

        mock_response_path = '/tests/resources/workflows/get_data_studio_workflows_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        self.data_studio_service.workflow_repository.get_data_studio_workflows = MagicMock(return_value=mock_response_items)

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = [
            Workflow(
                owner_id="test_owner_id",
                workflow_id="workflow_id",
                event_name="event_name",
                created_by="created_by_uuid",
                created_by_name="created_by_name",
                state="ACTIVE",
                version=1,
                is_sync_execution=True,
                state_machine_arn="state_machine_arn",
                is_binary_event=False,
                mapping_id="3eaddbdd-34cf-47fe-84fe-a0c971c6e4a6" 
            )
        ]

        self.assertListEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_should_handle_empty_response(self):
        """
        Tests if the function correctly returns an empty list when the owner doesn't have any data studio workflows.
        """
        owner_id = "test_owner_id"
        self.data_studio_service.workflow_repository.get_data_studio_workflows = MagicMock(return_value=[])

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = []

        self.assertEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)
    

    def test_get_workflows_should_raise_key_error_when_some_fields_are_missing_in_the_response(self):
        """
        Test if the function throws an error when some fields are missing in the response.
        Here mapping_id is missing in the response.
        """
        owner_id = "test_owner_id"
        mock_response_path = '/tests/resources/workflows/get_data_studio_workflows_response_with_some_missing_fields.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        self.data_studio_service.workflow_repository.get_data_studio_workflows = MagicMock(return_value=mock_response_items)

        with self.assertRaises(KeyError):
            self.data_studio_service.get_workflows(owner_id)
        
        self.data_studio_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_should_throw_service_exception_when_get_data_studio_workflows_method_of_workflow_repository_throws_service_exception(self):
        """
        Test if the function throws a ServiceException when the get_data_studio_workflows method of the workflow_repository throws a ServiceException.
        """
        owner_id = "test_owner_id"
        self.data_studio_service.workflow_repository.get_data_studio_workflows = MagicMock()
        self.data_studio_service.workflow_repository.get_data_studio_workflows.side_effect = ServiceException(400, ServiceStatus.FAILURE, 'Test Error')

        with self.assertRaises(ServiceException):
            self.data_studio_service.get_workflows(owner_id)
        
        self.data_studio_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)
        

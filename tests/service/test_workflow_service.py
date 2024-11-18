import unittest
from unittest.mock import MagicMock

from enums import ServiceStatus
from exception import ServiceException
from model import Workflow
from service import WorkflowService
from tests import TestUtils


class TestWorkflowService(unittest.TestCase):


    def setUp(self) -> None:
        self.workflow_repository = MagicMock()
        self.workflow_service = WorkflowService(self.workflow_repository)


    def tearDown(self) -> None:
        self.workflow_repository = None
        self.dashboard_service = None


    def test_get_data_studio_workflows_should_return_list_of_workflows(self):
        """
        Test if the function correctly returns the list of workflows for a given owner.
        """
        owner_id = "test_owner_id"

        mock_response_path = '/tests/resources/workflows/get_data_studio_workflows_response.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        self.workflow_service.workflow_repository.get_data_studio_workflows = MagicMock(return_value=mock_response_items)

        actual_result = self.workflow_service.get_data_studio_workflows(owner_id)
        expected_result = [
            Workflow(
                owner_id="test_owner_id",
                workflow_id="workflow_id",
                event_name="event_name",
                created_by="created_by_uuid",
                created_by_name="created_by_name",
                group_name="group_name",
                state="ACTIVE",
                version=1,
                is_sync_execution=True,
                state_machine_arn="state_machine_arn",
                is_binary_event=False,
                name="workflow_name",
                creation_date="2024-03-30T01:22:50.846714"
            )
        ]

        self.assertListEqual(expected_result, actual_result)
        self.workflow_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)


    def test_get_data_studio_workflows_should_handle_empty_response(self):
        """
        Tests if the function correctly returns an empty list when the owner doesn't have any data studio workflows.
        """
        owner_id = "test_owner_id"
        self.workflow_service.workflow_repository.get_data_studio_workflows = MagicMock(return_value=[])

        actual_result = self.workflow_service.get_data_studio_workflows(owner_id)
        expected_result = []

        self.assertEqual(expected_result, actual_result)
        self.workflow_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)
    

    def test_get_data_studio_workflows_should_raise_key_error_when_some_fields_are_missing_in_the_response(self):
        """
        Test if the function throws an error when some fields are missing in the response.
        Here mapping_id is missing in the response.
        """
        owner_id = "test_owner_id"
        mock_response_path = '/tests/resources/workflows/get_data_studio_workflows_response_with_some_missing_fields.json'
        mock_response_items = TestUtils.get_file_content(mock_response_path)
        self.workflow_service.workflow_repository.get_data_studio_workflows = MagicMock(return_value=mock_response_items)

        with self.assertRaises(KeyError):
            self.workflow_service.get_data_studio_workflows(owner_id)
        
        self.workflow_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)


    def test_get_data_studio_workflows_should_throw_service_exception_when_get_data_studio_workflows_method_of_workflow_repository_throws_service_exception(self):
        """
        Test if the function throws a ServiceException when the get_data_studio_workflows method of the workflow_repository throws a ServiceException.
        """
        owner_id = "test_owner_id"
        self.workflow_service.workflow_repository.get_data_studio_workflows = MagicMock()
        self.workflow_service.workflow_repository.get_data_studio_workflows.side_effect = ServiceException(400, ServiceStatus.FAILURE, 'Test Error')

        with self.assertRaises(ServiceException):
            self.workflow_service.get_data_studio_workflows(owner_id)
        
        self.workflow_service.workflow_repository.get_data_studio_workflows.assert_called_once_with(owner_id)


    def test_get_workflow_success(self):
        """Test that workflow is retrieved successfully from the service."""
        owner_id = "owner001"
        workflow_id = "workflow001"
        mock_response_path = "/tests/resources/workflows/get_data_studio_workflows_response.json"
        mock_response_items = TestUtils.get_file_content(mock_response_path)

        mock_workflow = Workflow.from_dict(mock_response_items[0])
        self.workflow_service.workflow_repository.get_workflow = MagicMock(return_value=mock_workflow)

        actual_result = self.workflow_service.get_workflow(owner_id, workflow_id)

        self.assertEqual(mock_workflow, actual_result)
        self.workflow_service.workflow_repository.get_workflow.assert_called_once_with(owner_id, workflow_id)


    def test_get_workflow_should_return_none(self):
        """Test that None is returned by the service when there is no workflow present in database."""
        owner_id = "owner001"
        workflow_id = "workflow001"
        self.workflow_service.workflow_repository.get_workflow = MagicMock(return_value=None)

        actual_result = self.workflow_service.get_workflow(owner_id, workflow_id)

        self.assertIsNone(actual_result)
        self.workflow_service.workflow_repository.get_workflow.assert_called_once_with(owner_id, workflow_id)


    def test_get_workflow_should_throw_service_exception(self):
        """Test that a ServiceException is raised when there is an error retrieving workflow."""
        owner_id = "owner001"
        workflow_id = "workflow001"
        self.workflow_service.workflow_repository.get_workflow = MagicMock()
        self.workflow_service.workflow_repository.get_workflow.side_effect = ServiceException(500, ServiceStatus.FAILURE, 'Error while retrieving workfow')

        with self.assertRaises(ServiceException) as context:
            self.workflow_service.get_workflow(owner_id, workflow_id)
        
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Error while retrieving workfow')
        self.workflow_service.workflow_repository.get_workflow.assert_called_once_with(owner_id, workflow_id)

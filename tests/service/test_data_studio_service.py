from decimal import Decimal
import unittest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError

from model import DataStudioWorkflow
from repository import WorkflowRepository
from service import DataStudioService


class TestDataStudioService(unittest.TestCase):


    def setUp(self) -> None:
        self.mock_table = MagicMock()
        self.app_config = MagicMock()
        self.aws_config = MagicMock()
        self.workflow_repository = WorkflowRepository(self.app_config, self.aws_config)
        self.data_studio_service = DataStudioService(self.workflow_repository)
        self.maxDiff = None


    def tearDown(self) -> None:
        self.app_config = None
        self.aws_config = None
        self.workflow_repository = None
        self.dashboard_service = None


    def test_get_workflows_should_return_list_of_workflows(self):
        """
        Test if the function correctly returns the list of workflows for a given owner.
        """
        owner_id = "test_owner_id"
        mock_response_items = [
            {
                "createdByName": "created_by_name",
                "version": Decimal("1"),
                "config": {"connections": [], "nodes": []},
                "is_binary_event": False,
                "is_sync_execution": True,
                "groupName": "group_name",
                "state_machine_arn": "state_machine_arn",
                "creationDate": "2024-03-30T01:22:50.846714",
                "createdBy": "created_by_uuid",
                "name": "workflow_name",
                "ownerId": "test_owner_id",
                "state": "ACTIVE",
                "workflowId": "workflow_id",
                "event_name": "event_name",
                "mapping_id": "3eaddbdd-34cf-47fe-84fe-a0c971c6e4a6"
            },
        ]
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=mock_response_items)

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = [
            DataStudioWorkflow(
                owner_id="test_owner_id",
                workflow_id="workflow_id",
                event_name="event_name",
                created_by="created_by_uuid",
                created_by_name="created_by_name",
                last_updated=None,
                state="ACTIVE",
                version=1,
                is_sync_execution=True,
                state_machine_arn="state_machine_arn",
                is_binary_event=False,
                mapping_id="3eaddbdd-34cf-47fe-84fe-a0c971c6e4a6" 
            )
        ]

        self.assertListEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.find_datastudio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_should_handle_empty_response(self):
        """
        Test if the function correctly handles an empty response.
        """
        owner_id = "test_owner_id"
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=[])

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = []

        self.assertEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.find_datastudio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_should_handle_missing_optional_fields(self):
        """
        Test if the function correctly handles workflows with missing optional fields.
        """
        owner_id = "test_owner_id"
        mock_response_items = [
            {
                "createdByName": "created_by_name",
                "version": Decimal("2"),
                "ownerId": "test_owner_id",
                "state": "ACTIVE",
                "workflowId": "workflow_id",
                "event_name": "event_name",
                # createdBy, state_machine_arn, etc. are missing
            }
        ]
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=mock_response_items)

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = [
            DataStudioWorkflow(
                owner_id="test_owner_id",
                workflow_id="workflow_id",
                event_name="event_name",
                created_by=None,
                created_by_name="created_by_name",
                last_updated=None,
                state="ACTIVE",
                version=2,
                is_sync_execution=None,
                state_machine_arn=None,
                is_binary_event=None,
                mapping_id=None,
            )
        ]

        self.assertEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.find_datastudio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_should_throw_client_error_when_owner_id_is_null(self):
        """
        Test if the function throws a ClientError when the owner_id is None.
        """
        owner_id = None
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock()
        self.data_studio_service.workflow_repository.find_datastudio_workflows.side_effect = ClientError({'Error': {'Message': 'Test Error'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}, 'find_datastudio_workflows')

        with self.assertRaises(ClientError):
            self.data_studio_service.get_workflows(owner_id)
        

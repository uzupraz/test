from decimal import Decimal
import unittest
from unittest.mock import MagicMock

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


    def test_get_workflows(self):
        """
        Test if the function correctly returns the list of workflows for a given owner.
        """
        owner_id = "test_owner_id"
        mock_response_items = [
            {
                "createdByName": "taskin",
                "version": Decimal("1"),
                "config": {"connections": [], "nodes": []},
                "is_binary_event": False,
                "is_sync_execution": True,
                "groupName": "Taskin",
                "state_machine_arn": "arn:aws:states:eu-central-1:451445658243:stateMachine:Taskin-JSON-ITC-WA",
                "creationDate": "2024-03-30T01:22:50.846714",
                "createdBy": "e5e086e2-2092-471e-8497-52ba7bf31ef6",
                "name": "Workflow to convert JSON into WA ITC.",
                "ownerId": "test_owner_id",
                "state": "ACTIVE",
                "workflowId": "KZlnumlwuVqnMoNGC9Rrj",
                "event_name": "es:workflow:test_owner_id:KZlnumlwuVqnMoNGC9Rrj",
                "mapping_id": "3eaddbdd-34cf-47fe-84fe-a0c971c6e4a6"
            },
        ]
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=mock_response_items)

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = [
            DataStudioWorkflow(
                owner_id="test_owner_id",
                workflow_id="KZlnumlwuVqnMoNGC9Rrj",
                event_name="es:workflow:test_owner_id:KZlnumlwuVqnMoNGC9Rrj",
                created_by="e5e086e2-2092-471e-8497-52ba7bf31ef6",
                created_by_name="taskin",
                last_updated=None,
                state="ACTIVE",
                version=1,
                is_sync_execution=True,
                state_machine_arn="arn:aws:states:eu-central-1:451445658243:stateMachine:Taskin-JSON-ITC-WA",
                is_binary_event=False,
                mapping_id="3eaddbdd-34cf-47fe-84fe-a0c971c6e4a6" 
            )
        ]

        self.assertListEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.find_datastudio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_empty_response(self):
        """
        Test if the function correctly handles an empty response.
        """
        owner_id = "test_owner_id"
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=[])

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = []

        self.assertEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.find_datastudio_workflows.assert_called_once_with(owner_id)


    def test_get_workflows_missing_optional_fields(self):
        """
        Test if the function correctly handles workflows with missing optional fields.
        """
        owner_id = "test_owner_id"
        mock_response_items = [
            {
                "createdByName": "taskin",
                "version": Decimal("2"),
                "ownerId": "test_owner_id",
                "state": "ACTIVE",
                "workflowId": "KZlnumlwuVqnMoNGC9Rrj",
                "event_name": "es:workflow:test_owner_id:KZlnumlwuVqnMoNGC9Rrj",
                # createdBy, state_machine_arn, etc. are missing
            }
        ]
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=mock_response_items)

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = [
            DataStudioWorkflow(
                owner_id="test_owner_id",
                workflow_id="KZlnumlwuVqnMoNGC9Rrj",
                event_name="es:workflow:test_owner_id:KZlnumlwuVqnMoNGC9Rrj",
                created_by=None,
                created_by_name="taskin",
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


    def test_get_workflows_null_owner_id(self):
        """
        Test if the function correctly handles a null owner_id.
        """
        owner_id = None
        self.data_studio_service.workflow_repository.find_datastudio_workflows = MagicMock(return_value=[])

        actual_result = self.data_studio_service.get_workflows(owner_id)
        expected_result = []

        self.assertEqual(expected_result, actual_result)
        self.data_studio_service.workflow_repository.find_datastudio_workflows.assert_called_once_with(owner_id)

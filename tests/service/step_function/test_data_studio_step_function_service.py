import unittest
from unittest.mock import MagicMock, patch
from exception import ServiceException
from enums import ServiceStatus
from service import DataStudioStepFunctionService
from utils import Singleton


class TestDataStudioStepFunctionService(unittest.TestCase):

    @patch("service.step_function.step_function_service.boto3.client")
    def setUp(self, mock_boto3_client) -> None:
        self.stepfunction = MagicMock()
        mock_boto3_client.return_value = self.stepfunction

        self.aws_config = MagicMock()
        self.aws_config.cloudwatch_log_group_base = "/test/log-group"
        self.aws_config.stepfunction_execution_role_arn = "arn:aws:iam::account-id:role/ExecutionRole"
        self.aws_config.cloudwatch_retention_in_days = 30
        self.aws_config.json_transformer_processor_arn = "arn:aws:lambda:region:account-id:function:JSONTransformer"

        self.aws_cloudwatch_service = MagicMock()
        self.data_formats_service = MagicMock()
        Singleton.clear_instance(DataStudioStepFunctionService)
        self.data_studio_step_function_service = DataStudioStepFunctionService(
            aws_config=self.aws_config,
            aws_cloudwatch_service=self.aws_cloudwatch_service,
            data_formats_service=self.data_formats_service
        )


    def tearDown(self) -> None:
        self.aws_config = None
        self.aws_cloudwatch_service = None
        self.data_formats_service = None
        self.data_studio_step_function_service = None
        Singleton.clear_instance(DataStudioStepFunctionService)


    def test_create_workflow_state_machine_success(self):
        """Test that a workflow state machine is created successfully."""
        mapping = MagicMock()
        mapping.owner_id = "owner123"
        mapping.id = "mapping123"
        mapping.description = "Test Workflow"

        self.data_studio_step_function_service.aws_config.cloudwatch_log_group_base = "/test/log-group"
        self.data_studio_step_function_service.get_data_studio_workflow_state_machine_definition = MagicMock(return_value={})
        self.data_studio_step_function_service.aws_cloudwatch_service.create_log_group = MagicMock(return_value="arn:aws:logs:region:account-id:log-group:test")
        self.data_studio_step_function_service.aws_cloudwatch_service.update_retention_policy = MagicMock()
        self.data_studio_step_function_service.aws_cloudwatch_service.get_logging_configuration = MagicMock(return_value={})
        self.data_studio_step_function_service.create_state_machine = MagicMock(return_value="arn:aws:states:region:account-id:stateMachine:TestWorkflow")

        result = self.data_studio_step_function_service.create_workflow_state_machine(mapping)

        self.assertEqual(result, "arn:aws:states:region:account-id:stateMachine:TestWorkflow")
        self.data_studio_step_function_service.aws_cloudwatch_service.create_log_group.assert_called_once_with("/test/log-group/owner123-mapping123-Logs")


    def test_create_workflow_state_machine_failure(self):
        """Test that a ServiceException is raised when workflow creation fails."""
        mapping = MagicMock()
        mapping.owner_id = "owner123"
        mapping.id = "mapping123"

        self.data_studio_step_function_service.get_data_studio_workflow_state_machine_definition = MagicMock(return_value={})
        self.data_studio_step_function_service.create_state_machine = MagicMock(side_effect=ServiceException(400, ServiceStatus.FAILURE, "Error"))

        with self.assertRaises(ServiceException) as context:
            self.data_studio_step_function_service.create_workflow_state_machine(mapping)

        self.assertEqual(context.exception.message, "Error")


    def test_update_workflow_state_machine_success(self):
        """Test that an existing workflow state machine is updated successfully."""
        mapping = MagicMock()
        workflow = MagicMock()
        workflow.state_machine_arn = "arn:aws:states:region:account-id:stateMachine:TestWorkflow"

        self.data_studio_step_function_service.get_data_studio_workflow_state_machine_definition = MagicMock(return_value={})
        self.data_studio_step_function_service.update_state_machine = MagicMock()

        self.data_studio_step_function_service.update_workflow_state_machine(mapping, workflow)

        self.data_studio_step_function_service.update_state_machine.assert_called_once()


    def test_update_workflow_state_machine_failure(self):
        """Test that a ServiceException is raised when workflow update fails."""
        mapping = MagicMock()
        workflow = MagicMock()
        workflow.state_machine_arn = "arn:aws:states:region:account-id:stateMachine:TestWorkflow"

        self.data_studio_step_function_service.get_data_studio_workflow_state_machine_definition = MagicMock(return_value={})
        self.data_studio_step_function_service.update_state_machine = MagicMock(side_effect=ServiceException(400, ServiceStatus.FAILURE, "Error"))

        with self.assertRaises(ServiceException) as context:
            self.data_studio_step_function_service.update_workflow_state_machine(mapping, workflow)

        self.assertEqual(context.exception.message, "Error")


    def test_get_parser_and_writer_details_success(self):
        """Test that parser and writer details are retrieved successfully."""
        mapping = MagicMock()
        mapping.sources = {"input": {"format": "JSON"}}
        mapping.output = {"format": "CSV"}

        mock_get_data_format_side_effect = [
            MagicMock(format_name="JSON", parser=MagicMock(lambda_arn="arn:aws:lambda:region:account-id:function:JSONParser")),
            MagicMock(format_name="CSV", writer=MagicMock(lambda_arn="arn:aws:lambda:region:account-id:function:CSVWriter"))
        ]
        self.data_studio_step_function_service.data_formats_service.get_data_format = MagicMock(
            side_effect = mock_get_data_format_side_effect
        )

        parser, writer = self.data_studio_step_function_service._DataStudioStepFunctionService__get_parser_and_writer_details(mapping)

        self.assertEqual(parser, ("JSON Parser", "arn:aws:lambda:region:account-id:function:JSONParser"))
        self.assertEqual(writer, ("CSV Writer", "arn:aws:lambda:region:account-id:function:CSVWriter"))


    def test_get_parser_and_writer_details_failure(self):
        """Test that a ServiceException is raised when parser or writer details are missing."""
        mapping = MagicMock()
        mapping.sources = {"input": {"format": None}}
        mapping.output = {"format": "CSV"}

        self.data_studio_step_function_service.data_formats_service.get_data_format = MagicMock(return_value=None)

        with self.assertRaises(ServiceException) as context:
            self.data_studio_step_function_service._DataStudioStepFunctionService__get_parser_and_writer_details(mapping)

        self.assertEqual(context.exception.message, "Invalid input or output format in mapping.")

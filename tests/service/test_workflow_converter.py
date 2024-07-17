import unittest
from parameterized import parameterized

from ..test_utils import TestUtils
from model import Workflow
from service import StepFunctionJSONConverter

@unittest.skip
class TestStepFunctionJSONConverter(unittest.TestCase):


    test_resource_path = '/tests/resources/workflow_converter/'


    def setUp(self) -> None:
        self.step_function_json_converter = StepFunctionJSONConverter()


    def tearDown(self) -> None:
        self.step_function_json_converter = None


    @parameterized.expand([
        ("task_node/custom_workflow_json_with_two_task_nodes.json", "task_node/custom_workflow_json_with_two_task_nodes_converted.json", None),
        ("parallel_node/custom_workflow_json_with_one_parallel_node.json", "parallel_node/custom_workflow_json_with_one_parallel_node_converted.json", None),
        ("parallel_node/custom_workflow_json_with_one_parallel_node_without_subworkflow.json", None, ValueError),
        ("map_node/custom_workflow_json_with_one_map_node.json", "map_node/custom_workflow_json_with_one_map_node_converted.json", None),
        ("map_node/custom_workflow_json_with_one_map_node_without_subworkflow.json", None, ValueError),
        ("wait_node/custom_workflow_json_with_one_wait_node.json", "wait_node/custom_workflow_json_with_one_wait_node_converted.json", None),
        ("wait_node/custom_workflow_json_with_one_wait_node_without_required_parameters.json", None, ValueError),
        ("wait_node/custom_workflow_json_with_one_wait_node_with_wrong_type_for_required_parameter.json", None, ValueError),
        ("custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections.json", "custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections_converted.json", None)
    ])
    def test_convert(self, input_file_name, expected_file_name, expected_exception):
        """
        Tests the all possible cases of converting a workflow into a step function JSON.
        """

        input_file_path = self.test_resource_path + input_file_name
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.from_dict(workflow_json)

        if expected_exception:
            with self.assertRaises(expected_exception):
                self.step_function_json_converter.convert(workflow)
        else:
            actual_result = self.step_function_json_converter.convert(workflow)

            expected_file_path = self.test_resource_path + expected_file_name
            expected_result = TestUtils.get_file_content(expected_file_path)

            self.assertEqual(expected_result, actual_result)

import unittest
import os

from ..test_utils import TestUtils
from model import Workflow
from service import StepFunctionJSONConverter


class TestStepFunctionJSONConverter(unittest.TestCase):


    test_resource_path = '/tests/resources/'


    def setUp(self) -> None:
        self.step_function_json_converter = StepFunctionJSONConverter()


    def tearDown(self) -> None:
        self.step_function_json_converter = None


    def test_convert_happy_case_with_node_type_as_task_should_successfully_convert_into_step_function_json(self):
        """
        Tests the happy case of converting a workflow with a node type of "task"
        into a step function JSON. Input workflow has two nodes and one connection between them.
        The second node is the end node.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_two_task_nodes.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)
        actual_result = self.step_function_json_converter.convert(workflow)

        expected_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_two_task_nodes_converted.json'
        expected_result = TestUtils.get_file_content(expected_file_path)

        self.assertEqual(expected_result, actual_result)


    def test_convert_with_node_type_as_parallel_should_successfully_convert_into_step_function_json(self):
        """
        Tests the happy case of converting a workflow with a node type of "parallel"
        into a step function JSON. Input workflow has two nodes and one connection between them.
        The second node is the parallel node. Parallel node includes a subworkflow where additional
        two task nodes are defined along with the one conncetion between them. The converted json should
        include the parallel state with two branches in it.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_parallel_node.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)
        actual_result = self.step_function_json_converter.convert(workflow)

        expected_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_parallel_node_converted.json'
        expected_result = TestUtils.get_file_content(expected_file_path)

        self.assertEqual(expected_result, actual_result)


    def test_convert_with_node_type_as_parallel_but_without_subworkflow_should_raise_exception(self):
        """
        Tests the case of converting a workflow with a node type of "parallel" but without a subworkflow
        into a step function JSON. This should raise an exception while converting.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_parallel_node_without_subworkflow.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        with self.assertRaises(Exception):
            self.step_function_json_converter.convert(workflow)


    def test_convert_with_node_type_as_map_should_successfully_convert_into_step_function_json(self):
        """
        Tests the happy case of converting a workflow with a node type of "map"
        into a step function JSON. Input workflow has two nodes (task and map) and one connection between them.
        The map node is the end node. Map node includes a subworkflow where additional
        two task nodes are defined along with the one conncetion between them. The converted json should
        include the map state with two task states in it inside the ItemProcessor.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_map_node.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)
        actual_result = self.step_function_json_converter.convert(workflow)

        expected_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_map_node_converted.json'
        expected_result = TestUtils.get_file_content(expected_file_path)

        self.assertEqual(expected_result, actual_result)


    def test_convert_with_node_type_as_map_but_without_subworkflow_should_raise_exception(self):
        """
        Tests the case of converting a workflow with a node type of "map" but without a subworkflow
        into a step function JSON. This should raise an exception while converting.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_map_node_without_subworkflow.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        with self.assertRaises(Exception):
            self.step_function_json_converter.convert(workflow)


    def test_convert_with_node_type_as_wait_should_successfully_convert_into_step_function_json(self):
        """
        Tests the happy case of converting a workflow with a node type of "wait"
        into a step function JSON. Input workflow has two nodes and one connection between them.
        The wait node is the end node. The expected json should include wait state along with value of 'Seconds' in it.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_wait_node.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)
        actual_result = self.step_function_json_converter.convert(workflow)

        expected_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_wait_node_converted.json'
        expected_result = TestUtils.get_file_content(expected_file_path)

        self.assertEqual(expected_result, actual_result)


    def test_convert_with_node_type_as_wait_but_without_required_parameters_should_raise_exception(self):
        """
        Tests the case of converting a workflow with a node type of "wait" but without required parameter (seconds)
        into a step function JSON. This should raise an exception while converting.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_wait_node_without_required_parameters.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        with self.assertRaises(Exception):
            self.step_function_json_converter.convert(workflow)


    def test_convert_with_node_type_as_wait_but_with_wrong_type_for_required_parameter_should_raise_exception(self):
        """
        Tests the case of converting a workflow with a node type of "wait" but with wrong type for required parameter (seconds)
        into a step function JSON. The parameter 'seconds' should be a number not a alphabetical.
        This should raise an exception while converting.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_one_wait_node_with_wrong_type_for_required_parameter.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)

        with self.assertRaises(Exception):
            self.step_function_json_converter.convert(workflow)


    def test_convert_with_multiple_nodes_of_different_types_and_multiple_connections_should_successfully_convert_into_step_function_json(self):
        """
        Tests the happy case of converting a workflow with multiple nodes of different types and multiple connections
        into a step function JSON. Input workflow has four nodes (task, paralell, map and wait) and three connections between them.
        The wait node is the end node.
        """

        input_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections.json'
        workflow_json = TestUtils.get_file_content(input_file_path)
        workflow = Workflow.parse_from(workflow_json)
        actual_result = self.step_function_json_converter.convert(workflow)

        expected_file_path = os.getcwd() + self.test_resource_path + 'custom_workflow_json_with_multiple_nodes_of_different_types_and_multiple_connections_converted.json'
        expected_result = TestUtils.get_file_content(expected_file_path)

        self.assertEqual(expected_result, actual_result)

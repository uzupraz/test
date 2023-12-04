from typing import List

from model import Workflow, Node, Connection
from controller import common_controller as common_ctrl


log = common_ctrl.log


class StepFunctionJSONConverter:
    """
    This class is used to convert a Workflow object into an AWS Step Functions state machine.
    Used only for enabling a workflow to be executed in AWS Step Functions.
    """


    def convert(self, workflow:Workflow) -> dict:
        log.info('Converting workflow into step function json. workflowId: %s', workflow.workflowId)
        try:
            step_function = {
                'Comment': workflow.name,
                'StartAt': workflow.config.startAt,
                'Version': workflow.workflowVersion,
                'States': self.__convert_states(workflow.config.nodes, workflow.config.connections)
            }
        except Exception as e:
            log.exception('Failed to convert workflow into step function json. workflowId: %s', workflow.workflowId, e)
            raise e

        log.info('Successfully converted workflow into step function json. workflowId: %s', workflow.workflowId)
        return step_function


    def __convert_subworkflow(self, sub_workflow:Workflow) -> None:
        sub_step_function = {
            'StartAt': sub_workflow.config.startAt,
            'States': self.__convert_states(sub_workflow.config.nodes, sub_workflow.config.connections)
        }

        return sub_step_function


    def __convert_states(self, nodes:List[Node], connections:List[Connection]):
        states = {}

        # Create states from nodes
        for node in nodes:
            state = self.__get_state(node)
            states[node.id] = state

        # Update state transitions based on connections
        for connection in connections:
            source_state = states[connection.sourceNode]
            source_state['Next'] = connection.targetNode

        # Find states that don't have a 'Next' field and add 'End': True
        for state in states.values():
            if 'Next' not in state:
                state['End'] = True

        return states


    def __get_state(self, node:Node):
        state = {
            'Type': node.type
        }

        if node.type == 'Task':
            state['Resource'] = node.nodeTemplateId
            state['Parameters'] = {
                'Payload.$': '$',  # Include this by default
                **node.parameters  # Include the rest of the parameters
            }
        elif node.type == 'Parallel':
            state['Branches'] = [self.__convert_subworkflow(node.subWorkflow)]
        elif node.type == 'Map':
            state['ItemProcessor'] = {
                'ProcessorConfig': {'Mode': 'INLINE'},
                'StartAt': node.subWorkflow.config.startAt,
                'States': self.__convert_states(node.subWorkflow.config.nodes, node.subWorkflow.config.connections)
            }
        elif node.type == 'Wait':
            state['Seconds'] = int(node.parameters['seconds'])

        return state

from model import Workflow


class WorkflowService():


    def __init__(self) -> None:
        pass


    def convert_to_step_function(workflow: Workflow):
        step_function = {
            "Comment": workflow.name,
            "StartAt": workflow.config.startAt,
            "Version": workflow.workflowVersion,
            "States": {}
        }

        # Create states from nodes
        for node in workflow.config.nodes:
            step_function['States'][node.id] = {
                "Type": "Task",
                "Resource": node.nodeTemplateId,
                "Parameters": node.parameters,
                "End": True  # Assume end state by default
            }

        # Update state transitions based on connections
        for connection in workflow.config.connections:
            source_state = step_function['States'][connection.sourceNode]
            source_state['Next'] = connection.targetNode
            source_state['End'] = False  # Not an end state since it has a next state

        return step_function
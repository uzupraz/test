from model import Workflow


class CustomWorkflowConverter:


    def convert(self, workflow: Workflow) -> dict:
        step_function = {
            "Comment": workflow.name,
            "StartAt": workflow.config.startAt,
            "Version": workflow.workflowVersion,
            "States": self.convert_states(workflow.config.nodes, workflow.config.connections)
        }

        return step_function


    def convert_subworkflow(self, sub_workflow: Workflow) -> None:
        sub_step_function = {
            "StartAt": sub_workflow.config.startAt,
            "States": self.convert_states(sub_workflow.config.nodes, sub_workflow.config.connections)
        }

        return sub_step_function


    def convert_states(self, nodes, connections):
        states = {}

        # Create states from nodes
        for node in nodes:
            state = self.__get_state(node)
            states[node.id] = state

        # Update state transitions based on connections
        for connection in connections:
            source_state = states[connection.sourceNode]
            if 'Next' not in source_state:
                source_state['Next'] = connection.targetNode
                if 'End' in source_state:
                    del source_state['End']  # Not an end state since it has a next state

        return states


    def __get_state(self, node):
        state = {
            "Type": node.type,
            "End": True
        }

        if node.type == "Task":
            state["Resource"] = node.nodeTemplateId
            state["Parameters"] = {
                "Payload.$": "$",  # Include this by default
                **node.parameters  # Include the rest of the parameters
            }
        elif node.type == "Parallel":
            state["Branches"] = [self.convert_subworkflow(node.subWorkflow)]
        elif node.type == "Map":
            state["ItemProcessor"] = {
                "ProcessorConfig": {"Mode": "INLINE"},
                "StartAt": node.subWorkflow.config.startAt,
                "States": self.convert_states(node.subWorkflow.config.nodes, node.subWorkflow.config.connections)
            }
        elif node.type == "Wait":
            state["Seconds"] = int(node.parameters["Seconds"])

        return state
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from dacite import from_dict


@dataclass
class StateMachineCreatePayload:
    state_machine_name: str
    state_machine_defination: dict
    execution_role_arn: str
    type: str = field(default="EXPRESS")
    logging_configuration: Optional[dict] = field(default=None)


@dataclass
class StateMachineUpdatePayload:
    state_machine_arn: str
    state_machine_defination: dict
    execution_role_arn: str
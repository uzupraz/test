from dataclasses import dataclass
from typing import List

@dataclass
class Module:
    module_name: str
    version: str

@dataclass
class TargetList:
    module_name: str
    version: str
    presigned_url: str
    checksum: str

@dataclass
class UpdateResponse:
    target_list: List[TargetList]
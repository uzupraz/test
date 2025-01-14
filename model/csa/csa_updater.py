from dataclasses import dataclass
from typing import List


@dataclass
class Module:
    module_name: str
    version: str


@dataclass
class Targets:
    module_name: str
    version: str
    presigned_url: str
    checksum: str


@dataclass
class UpdateRequest:
    owner_id: str
    machine_id: str
    modules: List[Module]


@dataclass
class UpdateResponse:
    targets: List[Targets]


@dataclass
class MachineInfo:
    owner_id: str
    machine_id: str
    platform: str
    modules: List[Module]


@dataclass
class ModuleInfo:
    module_name: str
    version: str
    checksum: str
import time

from dataclasses import dataclass, field
from typing import List, Union


@dataclass
class CustomScriptRelease:
    version_id: str
    edited_by: str
    source_version_id: str
    release_date: int = field(default=int(time.time()))


@dataclass
class CustomScriptUnPublishedChange:
    version_id: str
    edited_by: str
    source_version_id: Union[str, None] = field(default=None)
    edited_at: int = field(default=int(time.time()))


@dataclass
class CustomScript:
    owner_id: str
    script_id: str
    language: str
    extension: str
    name: str
    releases: List[CustomScriptRelease]
    unpublished_changes: List[CustomScriptUnPublishedChange]
    creation_date: int = field(default=int(time.time()))


@dataclass
class CreateCustomScriptRequestDTO:
    language: str
    extension: str
    name: str


@dataclass
class SaveCustomScriptRequestDTO:
    script: str
    script_id: Union[str, None] = None
    metadata: Union[CreateCustomScriptRequestDTO, None] = None
    source_version_id: Union[str, None] = None

    def __post_init__(self):
        if not self.script_id and not self.metadata:
            raise ValueError("Missing script metadata.")
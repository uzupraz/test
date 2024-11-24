import nanoid
import time

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ChatMessage:
    timestamp: int
    prompt: Optional[str] = None
    response: Optional[str] = None


@dataclass
class MessageHistoryPagination:
    size: int
    last_evaluated_key: str | None


@dataclass
class ChatResponse:
    chat_id: str
    created_at: int
    title: str


@dataclass
class MessageHistoryResponse:
    messages: List[ChatMessage]
    pagination: MessageHistoryPagination


@dataclass
class Chat:
    user_id: str
    owner_id: str
    model_id: str
    chat_id: str = field(init=False)
    timestamp: int = field(init=False)

    def __post_init__(self):
        self.chat_id = nanoid.generate()
        self.timestamp = int(time.time())


@dataclass
class ChatSession:
    chat_id: str
    timestamp: int 
    title: str = "" 


@dataclass
class SaveChatResponse:
    chat_id: str


@dataclass
class ChatMessageResponse:
    messages: List[ChatMessage]
    last_evaluated_key: Optional[dict]


@dataclass
class ChildChaInfo:
    chat_id: str
    prompt: str
    response: str
    timestamp: int = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time())


@dataclass
class Message:
    role: str
    content: str


@dataclass
class Messages:
    messages: List[Message]


@dataclass
class ParentChatInfo:
    model_id: str
    title: str = ""


@dataclass
class GenerateModelResponse:
    user_id: str
    chat_id: str
    prompt: str


@dataclass
class ChatCreationDate:
    timestamp: int


@dataclass
class GenerateModelRequest:
    anthropic_version: str
    max_tokens: int
    messages: List[Message]
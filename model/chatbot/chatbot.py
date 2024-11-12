import time

from dataclasses import dataclass, field
from typing import List


@dataclass
class Message:
    prompt: str
    response: str
    timestamp: int


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
    messages: List[Message]
    pagination: MessageHistoryPagination


@dataclass
class Chat:
    chat_id: str
    user_id: str
    owner_id: str
    timestamp: int = field(default=int(time.time()))
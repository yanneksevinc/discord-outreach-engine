from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


@dataclass
class Message:
    conversation_id: int
    content: str
    direction: str = "inbound"
    message_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conversation_id": self.conversation_id,
            "content": self.content,
            "direction": self.direction,
            "message_id": self.message_id,
            "created_at": self.created_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        return cls(
            conversation_id=int(data["conversation_id"]),
            content=str(data["content"]),
            direction=str(data.get("direction", "inbound")),
            message_id=data.get("message_id"),
            created_at=float(data.get("created_at", time.time())),
            metadata=dict(data.get("metadata", {})),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


class AIClient:
    def __init__(self, model: str = "gpt-4.1-mini", delay_seconds: float = 2.0):
        self.model = model
        self.delay_seconds = delay_seconds

    def think(self, message: Message, context: Optional[Dict[str, Any]] = None) -> str:
        context = context or {}
        prompt = context.get("prompt")
        if prompt:
            return f"{prompt}: {message.content}"
        return message.content

    def reply(self, message: Message, context: Optional[Dict[str, Any]] = None) -> Message:
        time.sleep(max(0.0, self.delay_seconds))
        reply_text = self.think(message, context)
        return Message(
            conversation_id=message.conversation_id,
            content=reply_text,
            direction="outbound",
            metadata={"model": self.model, "reply_to": message.message_id},
        )


class MessageQueue:
    def __init__(self, ai_client: AIClient, base_delay_seconds: float = 1.5, max_delay_seconds: float = 15.0):
        self.ai_client = ai_client
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self._queue: "queue.Queue[Message]" = queue.Queue()
        self._interrupt = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._burst_count = 0

    def push(self, message: Message) -> None:
        self._queue.put(message)
        self._interrupt.set()

    def interrupt(self) -> None:
        self._interrupt.set()

    def stop(self) -> None:
        self._stop.set()
        self._interrupt.set()

    def _compute_delay(self) -> float:
        with self._lock:
            delay = min(self.max_delay_seconds, self.base_delay_seconds * (1 + self._burst_count))
            self._burst_count += 1
            return delay

    def _sleep_with_interrupt(self, seconds: float) -> bool:
        deadline = time.time() + max(0.0, seconds)
        while not self._stop.is_set():
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            if self._interrupt.wait(timeout=min(0.25, remaining)):
                self._interrupt.clear()
                return True
        return True

    def run(self, handler: Callable[[Message], Optional[Message]]) -> None:
        while not self._stop.is_set():
            try:
                message = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                result = handler(message)
                if result is None:
                    result = self.ai_client.reply(message)
                self._emit(result)
            finally:
                self._queue.task_done()

            if self._sleep_with_interrupt(self._compute_delay()):
                continue

    def _emit(self, message: Message) -> None:
        # Replace this with persistence or a network transport.
        print(message.to_json())

    def drain(self, handler: Callable[[Message], Optional[Message]]) -> None:
        worker = threading.Thread(target=self.run, args=(handler,), daemon=True)
        worker.start()
        self._queue.join()
        self.stop()
        worker.join(timeout=1.0)


if __name__ == "__main__":
    client = AIClient()
    queue_ = MessageQueue(client)

    def handler(message: Message) -> Optional[Message]:
        return client.reply(message, {"prompt": "ack"})

    queue_.push(Message(conversation_id=1, content="hello", message_id="msg_1"))
    queue_.drain(handler)

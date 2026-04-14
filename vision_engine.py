from __future__ import annotations

import base64
import json
import os
import sqlite3
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None


DEFAULT_DASHSCOPE_BASE_URL = os.getenv("DASHSCOPE_BASE_URL", "https://dashscope-intl.aliyuncs.com/api/v1")
DEFAULT_DASHSCOPE_MODEL = os.getenv("DASHSCOPE_VISION_MODEL", "qwen-vl-plus")
DEFAULT_DASHSCOPE_TIMEOUT = float(os.getenv("DASHSCOPE_TIMEOUT", "30"))
DEFAULT_DASHSCOPE_MAX_TOKENS = int(os.getenv("DASHSCOPE_MAX_TOKENS", "128"))
DEFAULT_DASHSCOPE_TEMPERATURE = float(os.getenv("DASHSCOPE_TEMPERATURE", "0.2"))
DEFAULT_PROMPT = (
    "Describe this image for a Discord DM assistant in one short sentence. "
    "Focus on the main visible content, text if it matters, and any relevant mood or action. "
    "Keep it concise and neutral."
)


@dataclass(slots=True)
class VisionObservation:
    attachment_url: str
    description: str
    mime_type: str | None = None
    is_gif: bool = False


@dataclass(slots=True)
class MessageContext:
    role: str
    content: str
    message_id: int | None = None
    attachment_url: str | None = None
    vision_description: str | None = None


class VisionError(RuntimeError):
    pass


class VisionInterpreter:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = DEFAULT_DASHSCOPE_BASE_URL,
        model: str = DEFAULT_DASHSCOPE_MODEL,
        timeout: float = DEFAULT_DASHSCOPE_TIMEOUT,
        max_tokens: int = DEFAULT_DASHSCOPE_MAX_TOKENS,
        temperature: float = DEFAULT_DASHSCOPE_TEMPERATURE,
        prompt: str = DEFAULT_PROMPT,
    ) -> None:
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY") or os.getenv("DASHSCOPE_API_TOKEN")
        if not self.api_key:
            raise ValueError("DashScope API key not configured. Set DASHSCOPE_API_KEY.")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.prompt = prompt

    def describe_image_url(self, image_url: str) -> str:
        payload_url = self._prepare_vision_source(image_url)
        response = self._call_dashscope(payload_url)
        description = self._extract_description(response)
        return self._clean_description(description)

    def describe_attachment(self, attachment_url: str, mime_type: str | None = None) -> VisionObservation:
        is_gif = self._looks_like_gif(attachment_url, mime_type)
        source_url = attachment_url
        if is_gif:
            source_url = self._gif_first_frame_source(attachment_url)
        response = self._call_dashscope(source_url)
        description = self._clean_description(self._extract_description(response))
        return VisionObservation(
            attachment_url=attachment_url,
            description=description,
            mime_type=mime_type,
            is_gif=is_gif,
        )

    def annotate_message_content(
        self,
        content: str,
        attachment_url: str | None = None,
        *,
        mime_type: str | None = None,
    ) -> str:
        if not attachment_url:
            return content
        observation = self.describe_attachment(attachment_url, mime_type=mime_type)
        note = f"[vision: {observation.description}]"
        if content.strip():
            return f"{content.rstrip()}
{note}"
        return note

    def _prepare_vision_source(self, image_url: str) -> str:
        if self._looks_like_gif(image_url, None):
            return self._gif_first_frame_source(image_url)
        return image_url

    def _looks_like_gif(self, image_url: str, mime_type: str | None) -> bool:
        if mime_type and mime_type.lower() == "image/gif":
            return True
        parsed = urlparse(image_url)
        return parsed.path.lower().endswith(".gif")

    def _gif_first_frame_source(self, image_url: str) -> str:
        raw = self._download_bytes(image_url)
        if Image is None:
            return image_url
        try:
            with Image.open(BytesIO(raw)) as img:
                img.seek(0)
                frame = img.convert("RGB")
                buffer = BytesIO()
                frame.save(buffer, format="PNG")
                encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
                return f"data:image/png;base64,{encoded}"
        except Exception:
            return image_url

    def _call_dashscope(self, image_source: str) -> dict[str, Any]:
        url = f"{self.base_url}/services/aigc/multimodal-generation/generation"
        body = {
            "model": self.model,
            "input": {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"image": image_source},
                            {"text": self.prompt},
                        ],
                    }
                ]
            },
            "parameters": {
                "temperature": self.temperature,
                "max_tokens": self.max_tokens,
            },
        }
        request = Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            raise VisionError(f"DashScope request failed with HTTP {exc.code}: {error_body or exc.reason}") from exc
        except URLError as exc:
            raise VisionError(f"DashScope request failed: {exc.reason}") from exc
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise VisionError(f"DashScope returned invalid JSON: {raw[:500]}") from exc

    def _extract_description(self, response: dict[str, Any]) -> str:
        if isinstance(response.get("output"), str):
            return str(response["output"])
        output = response.get("output")
        if isinstance(output, dict):
            for key in ("text", "content", "message"):
                extracted = self._extract_text_from_any(output.get(key))
                if extracted:
                    return extracted
            choices = output.get("choices")
            if isinstance(choices, list):
                for choice in choices:
                    extracted = self._extract_text_from_any(choice)
                    if extracted:
                        return extracted
        for key in ("text", "result", "output_text"):
            extracted = self._extract_text_from_any(response.get(key))
            if extracted:
                return extracted
        raise VisionError(f"Could not find a description in DashScope response: {response}")

    def _extract_text_from_any(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            for key in ("text", "content", "output", "message"):
                extracted = self._extract_text_from_any(value.get(key))
                if extracted:
                    return extracted
            return None
        if isinstance(value, list):
            parts = [p for item in value if (p := self._extract_text_from_any(item))]
            if parts:
                return " ".join(parts)
        return None

    def _clean_description(self, text: str) -> str:
        cleaned = " ".join(str(text).replace("
", " ").split())
        if len(cleaned) > 280:
            cleaned = cleaned[:277].rstrip() + "..."
        return cleaned

    def _download_bytes(self, url: str) -> bytes:
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=self.timeout) as response:
            return response.read()


class SQLiteMessageContextBuilder:
    def __init__(self, db_path: str | Path, vision: VisionInterpreter | None = None) -> None:
        self.db_path = Path(db_path)
        self.vision = vision

    def build_context(self, conversation_id: int, *, limit: int | None = None) -> list[dict[str, Any]]:
        messages = self._load_messages(conversation_id, limit=limit)
        context: list[dict[str, Any]] = []
        for message in messages:
            content = str(message.get("content") or "")
            metadata = self._parse_metadata(message.get("metadata_json"))
            attachment = self._extract_attachment_url(metadata)
            mime_type = self._extract_attachment_mime_type(metadata)
            if attachment and self.vision is not None:
                content = self.vision.annotate_message_content(content, attachment, mime_type=mime_type)
            elif attachment and not content.strip():
                content = f"[attachment: {attachment}]"
            role = self._direction_to_role(str(message.get("direction") or "incoming"))
            context.append({
                "role": role,
                "content": content,
                "message_id": int(message["id"]) if message.get("id") is not None else None,
                "conversation_id": conversation_id,
                "sent_at": message.get("sent_at"),
                "attachment_url": attachment,
            })
        return context

    def _load_messages(self, conversation_id: int, *, limit: int | None = None) -> list[dict[str, Any]]:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        query = (
            "SELECT id, conversation_id, direction, content, metadata_json, sent_at "
            "FROM messages WHERE conversation_id = ? ORDER BY sent_at ASC, id ASC"
        )
        if limit is not None:
            query += " LIMIT ?"
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, (conversation_id,) if limit is None else (conversation_id, limit)).fetchall()
        return [dict(row) for row in rows]

    def _parse_metadata(self, raw: Any) -> dict[str, Any]:
        if raw in (None, ""):
            return {}
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _extract_attachment_url(self, metadata: dict[str, Any]) -> str | None:
        for key in ("attachment_url", "attachmentUrl", "url", "image_url", "imageUrl", "media_url", "mediaUrl"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        attachments = metadata.get("attachments")
        if isinstance(attachments, list):
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                for key in ("url", "proxy_url", "image_url", "thumbnail_url"):
                    value = attachment.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        return None

    def _extract_attachment_mime_type(self, metadata: dict[str, Any]) -> str | None:
        for key in ("mime_type", "mimeType", "content_type", "contentType"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        attachments = metadata.get("attachments")
        if isinstance(attachments, list):
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                for key in ("content_type", "contentType", "mime_type", "mimeType"):
                    value = attachment.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        return None

    def _direction_to_role(self, direction: str) -> str:
        return "assistant" if direction.strip().lower() in {"outbound", "assistant", "sent"} else "user"


class VisionAwareContextBuilder(SQLiteMessageContextBuilder):
    """Compatibility alias."""


__all__ = [
    "DEFAULT_DASHSCOPE_BASE_URL",
    "DEFAULT_DASHSCOPE_MODEL",
    "MessageContext",
    "SQLiteMessageContextBuilder",
    "VisionAwareContextBuilder",
    "VisionError",
    "VisionInterpreter",
    "VisionObservation",
]
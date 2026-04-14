from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from openai import OpenAI

DASHSCOPE_BASE_URL = "https://dashscope-intl.aliyuncs.com/api/v1"
DEFAULT_MODEL = "qwen-plus"
DEFAULT_SUMMARIZER_MODEL = "qwen-plus"


@dataclass
class ChatMessage:
    role: str
    content: str
    created_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SummaryRecord:
    summary: str
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    message_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class ResponseGenerator:
    """Generate outreach responses for the "Jannik, 23" persona."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = DASHSCOPE_BASE_URL,
        model: str = DEFAULT_MODEL,
        summarizer_model: str | None = None,
        max_recent_messages: int = 12,
        max_context_chars: int = 12000,
        summary_keep_messages: int = 8,
    ) -> None:
        resolved_key = api_key or os.getenv("DASHSCOPE_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not resolved_key:
            raise ValueError("Missing API key. Set DASHSCOPE_API_KEY or OPENAI_API_KEY.")

        self.client = OpenAI(api_key=resolved_key, base_url=base_url)
        self.model = model
        self.summarizer_model = summarizer_model or model or DEFAULT_SUMMARIZER_MODEL
        self.max_recent_messages = max_recent_messages
        self.max_context_chars = max_context_chars
        self.summary_keep_messages = summary_keep_messages

    def build_prompt(
        self,
        *,
        user_profiles: Any,
        messages: Sequence[Any],
        context_summaries: Sequence[Any] | None = None,
        mode: str = "final",
    ) -> list[dict[str, str]]:
        profiles = self._normalize_profiles(user_profiles)
        summaries = self._normalize_summaries(context_summaries or [])
        recent_messages, trimmed_summaries = self._compress_context(messages, summaries)

        system_prompt = self._build_system_prompt(profiles, trimmed_summaries, mode=mode)
        prompt_messages = [{"role": "system", "content": system_prompt}]

        if recent_messages:
            for item in recent_messages:
                prompt_messages.append(
                    {
                        "role": item.role,
                        "content": self._format_message_content(item),
                    }
                )
        else:
            prompt_messages.append(
                {
                    "role": "user",
                    "content": "No recent chat messages are available. Use the profile data and context summaries to continue naturally.",
                }
            )

        return prompt_messages

    def generate_two_suggestions(
        self,
        *,
        user_profiles: Any,
        messages: Sequence[Any],
        context_summaries: Sequence[Any] | None = None,
    ) -> list[str]:
        prompt_messages = self.build_prompt(
            user_profiles=user_profiles,
            messages=messages,
            context_summaries=context_summaries,
            mode="suggestions",
        )
        prompt_messages[-1]["content"] += (
            "\n\nReturn valid JSON only in this exact format: "
            '{"suggestions": ["suggestion one", "suggestion two"]}. '
            "The suggestions should be distinct, natural, and short enough to send in chat."
        )
        payload = self._chat_completion(
            prompt_messages,
            model=self.model,
            temperature=0.8,
        )
        data = self._parse_json(payload, default_key="suggestions")
        suggestions = data.get("suggestions") if isinstance(data, dict) else None
        if isinstance(suggestions, list):
            cleaned = [str(item).strip() for item in suggestions if str(item).strip()]
            if cleaned:
                return cleaned[:2]
        return self._fallback_suggestions(payload)

    def generate_final_response(
        self,
        *,
        user_profiles: Any,
        messages: Sequence[Any],
        context_summaries: Sequence[Any] | None = None,
    ) -> str:
        prompt_messages = self.build_prompt(
            user_profiles=user_profiles,
            messages=messages,
            context_summaries=context_summaries,
            mode="final",
        )
        prompt_messages[-1]["content"] += (
            "\n\nReturn valid JSON only in this exact format: {\"response\": \"final reply text\"}. "
            "The response should sound like Jannik, 23: casual, confident, warm, and not overly polished."
        )
        payload = self._chat_completion(
            prompt_messages,
            model=self.model,
            temperature=0.7,
        )
        data = self._parse_json(payload, default_key="response")
        if isinstance(data, dict):
            response = data.get("response")
            if isinstance(response, str) and response.strip():
                return response.strip()
        return payload.strip()

    def summarize_older_messages(
        self,
        *,
        messages: Sequence[Any],
        existing_summaries: Sequence[Any] | None = None,
    ) -> list[SummaryRecord]:
        summaries = self._normalize_summaries(existing_summaries or [])
        recent_messages, updated_summaries = self._compress_context(messages, summaries, force_summary=True)
        return updated_summaries

    def _compress_context(
        self,
        messages: Sequence[Any],
        summaries: list[SummaryRecord],
        *,
        force_summary: bool = False,
    ) -> tuple[list[ChatMessage], list[SummaryRecord]]:
        normalized = [self._normalize_message(message) for message in messages]
        if not normalized:
            return [], summaries

        char_count = sum(len(message.content) for message in normalized)
        should_compact = force_summary or len(normalized) > self.max_recent_messages or char_count > self.max_context_chars
        if not should_compact:
            return normalized, summaries

        keep_count = min(self.summary_keep_messages, len(normalized))
        older_messages = normalized[:-keep_count] if keep_count < len(normalized) else normalized[: max(0, len(normalized) - 1)]
        recent_messages = normalized[-keep_count:] if keep_count else normalized

        if older_messages:
            summary_text = self._summarize_messages(older_messages, summaries)
            summaries = [*summaries, SummaryRecord(summary=summary_text, message_count=len(older_messages))]

        return recent_messages, summaries

    def _summarize_messages(self, messages: Sequence[ChatMessage], existing_summaries: Sequence[SummaryRecord]) -> str:
        messages_block = self._render_messages(messages)
        summary_block = "\n".join(
            f"- {summary.summary}" for summary in existing_summaries if summary.summary.strip()
        ).strip()

        system_prompt = (
            "You compress older chat history into a concise memory for future replies. "
            "Keep names, preferences, objections, promises, open questions, and any important tone cues. "
            "Do not add new facts. Keep it short, readable, and useful."
        )
        user_prompt = [
            "Existing context summaries:",
            summary_block or "None.",
            "\nOlder messages to compress:",
            messages_block,
            "\nReturn plain text only. Aim for 4-8 compact bullet points or a dense paragraph under ~900 characters.",
        ]
        response = self._chat_completion(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "\n".join(user_prompt)},
            ],
            model=self.summarizer_model,
            temperature=0.2,
        )
        return response.strip()

    def _build_system_prompt(
        self,
        profiles: list[dict[str, Any]],
        summaries: list[SummaryRecord],
        *,
        mode: str,
    ) -> str:
        profile_lines = self._render_profiles(profiles)
        summary_lines = self._render_summaries(summaries)
        output_rules = {
            "suggestions": (
                "When asked for suggestions, provide two distinct options. "
                "They should differ in tone or angle, but both must fit the conversation."
            ),
            "final": (
                "When asked for a final response, produce one message that is ready to send. "
                "Keep it natural, concise, and aligned with the persona."
            ),
        }.get(mode, "Produce the best possible reply.")

        return "\n".join(
            [
                "You are Jannik, 23.",
                "Persona: casual, direct, confident, and friendly. Write like a real person in chat, not like a marketer.",
                "Style: short paragraphs, simple language, no emojis unless the conversation already uses them, and no overexplaining.",
                "Behavior: respond to the latest message while respecting the user profile data, prior commitments, and the conversation history.",
                "Profile data:",
                profile_lines or "- No profile data provided.",
                "Context summaries:",
                summary_lines or "- No prior context summaries.",
                "Output rules:",
                output_rules,
            ]
        )

    def _chat_completion(
        self,
        messages: list[dict[str, str]],
        *,
        model: str,
        temperature: float,
    ) -> str:
        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        choice = response.choices[0]
        content = getattr(choice.message, "content", None)
        if isinstance(content, list):
            return "".join(str(part.get("text", part)) for part in content if part)
        return str(content or "")

    def _parse_json(self, text: str, *, default_key: str) -> dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`\n")
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        if default_key == "suggestions":
            suggestions = self._extract_lines(cleaned)
            if suggestions:
                return {"suggestions": suggestions[:2]}
        if default_key == "response" and cleaned:
            return {"response": cleaned}
        return {}

    def _fallback_suggestions(self, payload: str) -> list[str]:
        cleaned_lines = self._extract_lines(payload)
        if cleaned_lines:
            return cleaned_lines[:2]
        text = payload.strip()
        if text:
            return [text]
        return []

    def _extract_lines(self, text: str) -> list[str]:
        lines = []
        for raw_line in text.splitlines():
            line = raw_line.strip().lstrip("-•0123456789. ").strip()
            if line:
                lines.append(line)
        return lines

    def _normalize_profiles(self, user_profiles: Any) -> list[dict[str, Any]]:
        if user_profiles is None:
            return []
        if isinstance(user_profiles, dict):
            return [user_profiles]
        if isinstance(user_profiles, (str, bytes)):
            return [{"value": user_profiles.decode() if isinstance(user_profiles, bytes) else user_profiles}]
        if hasattr(user_profiles, "keys") and callable(getattr(user_profiles, "keys")):
            try:
                return [dict(user_profiles)]
            except Exception:
                pass
        if isinstance(user_profiles, Iterable):
            normalized: list[dict[str, Any]] = []
            for profile in user_profiles:
                if isinstance(profile, dict):
                    normalized.append(profile)
                elif hasattr(profile, "keys") and callable(getattr(profile, "keys")):
                    try:
                        normalized.append(dict(profile))
                    except Exception:
                        normalized.append(self._object_to_dict(profile))
                else:
                    normalized.append(self._object_to_dict(profile))
            return normalized
        return [self._object_to_dict(user_profiles)]

    def _normalize_summaries(self, context_summaries: Sequence[Any]) -> list[SummaryRecord]:
        normalized: list[SummaryRecord] = []
        for summary in context_summaries:
            if isinstance(summary, SummaryRecord):
                normalized.append(summary)
            elif isinstance(summary, dict):
                normalized.append(
                    SummaryRecord(
                        summary=str(summary.get("summary", summary.get("text", ""))).strip(),
                        created_at=str(summary.get("created_at") or datetime.now(timezone.utc).isoformat()),
                        message_count=int(summary.get("message_count", 0) or 0),
                        metadata=dict(summary.get("metadata", {})) if isinstance(summary.get("metadata", {}), dict) else {},
                    )
                )
            else:
                normalized.append(SummaryRecord(summary=str(summary).strip()))
        return [summary for summary in normalized if summary.summary]

    def _normalize_message(self, message: Any) -> ChatMessage:
        if isinstance(message, ChatMessage):
            return message
        if isinstance(message, dict):
            role = str(
                message.get("role")
                or self._direction_to_role(message.get("direction"))
                or "user"
            )
            content = str(
                message.get("content")
                or message.get("text")
                or message.get("body")
                or ""
            )
            created_at = message.get("created_at") or message.get("sent_at") or message.get("sentAt")
            metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else {}
            return ChatMessage(role=role, content=content, created_at=str(created_at) if created_at else None, metadata=metadata)

        content = getattr(message, "content", None) or getattr(message, "text", None) or ""
        role = getattr(message, "role", None) or self._direction_to_role(getattr(message, "direction", None)) or "user"
        created_at = getattr(message, "created_at", None) or getattr(message, "sent_at", None)
        metadata = getattr(message, "metadata", None) if isinstance(getattr(message, "metadata", None), dict) else {}
        return ChatMessage(role=str(role), content=str(content), created_at=str(created_at) if created_at else None, metadata=metadata)

    def _direction_to_role(self, direction: Any) -> str:
        if direction is None:
            return "user"
        direction_text = str(direction).lower()
        if direction_text in {"outbound", "assistant", "agent", "system"}:
            return "assistant"
        return "user"

    def _format_message_content(self, message: ChatMessage) -> str:
        if message.created_at:
            return f"{message.content}\n[sent_at: {message.created_at}]"
        return message.content

    def _render_profiles(self, profiles: list[dict[str, Any]]) -> str:
        lines: list[str] = []
        for index, profile in enumerate(profiles, start=1):
            pieces = []
            for key in (
                "username",
                "display_name",
                "bio",
                "location",
                "language",
                "timezone",
                "profile_url",
            ):
                value = profile.get(key)
                if value not in (None, ""):
                    pieces.append(f"{key}={value}")
            metadata = profile.get("metadata_json") or profile.get("metadata")
            if metadata not in (None, ""):
                if isinstance(metadata, (dict, list)):
                    metadata = json.dumps(metadata, ensure_ascii=False)
                pieces.append(f"metadata={metadata}")
            lines.append(f"- profile {index}: " + "; ".join(pieces) if pieces else f"- profile {index}: (empty)")
        return "\n".join(lines)

    def _render_summaries(self, summaries: list[SummaryRecord]) -> str:
        if not summaries:
            return ""
        lines = []
        for index, summary in enumerate(summaries, start=1):
            prefix = f"- summary {index}"
            details = summary.summary.strip().replace("\n", " ")
            if summary.message_count:
                lines.append(f"{prefix} ({summary.message_count} messages): {details}")
            else:
                lines.append(f"{prefix}: {details}")
        return "\n".join(lines)

    def _render_messages(self, messages: Sequence[ChatMessage]) -> str:
        lines = []
        for message in messages:
            role = message.role or "user"
            content = message.content.strip().replace("\n", " ")
            if message.created_at:
                lines.append(f"{role}: {content} [sent_at: {message.created_at}]")
            else:
                lines.append(f"{role}: {content}")
        return "\n".join(lines)

    def _object_to_dict(self, value: Any) -> dict[str, Any]:
        if hasattr(value, "to_dict") and callable(value.to_dict):
            maybe_dict = value.to_dict()
            if isinstance(maybe_dict, dict):
                return maybe_dict
        if hasattr(value, "keys") and callable(getattr(value, "keys")):
            try:
                return dict(value)
            except Exception:
                pass
        result: dict[str, Any] = {}
        for key in (
            "id",
            "account_id",
            "platform_user_id",
            "username",
            "display_name",
            "bio",
            "location",
            "language",
            "timezone",
            "profile_url",
            "metadata_json",
            "metadata",
        ):
            if hasattr(value, key):
                item = getattr(value, key)
                if item is not None:
                    result[key] = item
        if result:
            return result
        return {"value": str(value)}


__all__ = [
    "ChatMessage",
    "ResponseGenerator",
    "SummaryRecord",
]


if __name__ == "__main__":
    raise SystemExit(
        "ai_engine.py is a library module. Import ResponseGenerator from it rather than running it directly."
    )

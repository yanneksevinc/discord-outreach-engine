from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from dotenv import load_dotenv


@dataclass(slots=True)
class ProxyCredential:
    host: str
    port: int
    username: str | None = None
    password: str | None = None
    scheme: str = "http"

    @property
    def url(self) -> str:
        auth = ""
        if self.username:
            auth = self.username
            if self.password:
                auth += f":{self.password}"
            auth += "@"
        return f"{self.scheme}://{auth}{self.host}:{self.port}"


@dataclass(slots=True)
class AppConfig:
    root_dir: Path = field(default_factory=lambda: Path.cwd())
    config_path: Path = field(default_factory=lambda: Path("config.json"))
    env_path: Path = field(default_factory=lambda: Path(".env"))
    dashscope_api_key: str | None = None
    openai_api_key: str | None = None
    credentials: dict[str, Any] = field(default_factory=dict)
    proxies: list[ProxyCredential] = field(default_factory=list)
    db_path: Path = field(default_factory=lambda: Path(os.getenv("DOE_DB_PATH", "doe.db")))
    flask_host: str = "127.0.0.1"
    flask_port: int = 8000
    worker_threads: int = 2
    debug: bool = False
    secret_key: str = "change-me"

    @classmethod
    def load(cls, root_dir: str | Path | None = None) -> Self:
        root = Path(root_dir) if root_dir is not None else Path.cwd()
        config_path = root / "config.json"
        env_path = root / ".env"

        if env_path.exists():
            load_dotenv(env_path, override=False)
        else:
            load_dotenv(override=False)

        file_data = cls._load_json(config_path)

        cfg = cls(
            root_dir=root,
            config_path=config_path,
            env_path=env_path,
            dashscope_api_key=os.getenv("DASHSCOPE_API_KEY") or file_data.get("dashscope_api_key"),
            openai_api_key=os.getenv("OPENAI_API_KEY") or file_data.get("openai_api_key"),
            credentials=cls._merged_dict(file_data.get("credentials"), cls._load_json_dict_env("CREDENTIALS_JSON")),
            proxies=cls._load_proxies(file_data),
            db_path=Path(os.getenv("DOE_DB_PATH", file_data.get("db_path", "doe.db"))),
            flask_host=os.getenv("FLASK_HOST", file_data.get("flask_host", "127.0.0.1")),
            flask_port=int(os.getenv("FLASK_PORT", file_data.get("flask_port", 8000))),
            worker_threads=int(os.getenv("WORKER_THREADS", file_data.get("worker_threads", 2))),
            debug=cls._env_bool("DEBUG", bool(file_data.get("debug", False))),
            secret_key=os.getenv("FLASK_SECRET_KEY", file_data.get("secret_key", "change-me")),
        )

        env_proxies = cls._load_json_list_env("PROXIES_JSON")
        if env_proxies:
            cfg.proxies = cls._load_proxies({"proxies": env_proxies})

        return cfg

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _load_json_dict_env(name: str) -> dict[str, Any]:
        raw = os.getenv(name)
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _load_json_list_env(name: str) -> list[dict[str, Any]]:
        raw = os.getenv(name)
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    @staticmethod
    def _merged_dict(*parts: Any) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for part in parts:
            if isinstance(part, dict):
                merged.update(part)
        return merged

    @staticmethod
    def _load_proxies(file_data: dict[str, Any]) -> list[ProxyCredential]:
        raw = file_data.get("proxies", [])
        if not isinstance(raw, list):
            return []

        proxies: list[ProxyCredential] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            host = str(entry.get("host", "")).strip()
            port_value = entry.get("port", 0)
            try:
                port = int(port_value)
            except (TypeError, ValueError):
                continue
            if not host or port <= 0:
                continue
            proxies.append(
                ProxyCredential(
                    host=host,
                    port=port,
                    username=entry.get("username") or None,
                    password=entry.get("password") or None,
                    scheme=str(entry.get("scheme", "http")),
                )
            )
        return proxies

    def as_dict(self) -> dict[str, Any]:
        return {
            "root_dir": str(self.root_dir),
            "config_path": str(self.config_path),
            "env_path": str(self.env_path),
            "dashscope_api_key": self.dashscope_api_key,
            "openai_api_key": self.openai_api_key,
            "credentials": self.credentials,
            "proxies": [
                {
                    "host": proxy.host,
                    "port": proxy.port,
                    "username": proxy.username,
                    "password": proxy.password,
                    "scheme": proxy.scheme,
                    "url": proxy.url,
                }
                for proxy in self.proxies
            ],
            "db_path": str(self.db_path),
            "flask_host": self.flask_host,
            "flask_port": self.flask_port,
            "worker_threads": self.worker_threads,
            "debug": self.debug,
            "secret_key": self.secret_key,
        }

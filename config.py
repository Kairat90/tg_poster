import os
import sys
from dataclasses import dataclass
from pathlib import Path


def get_runtime_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = get_runtime_base_dir()
MEDIA_DIR = BASE_DIR / "media"
DB_PATH = BASE_DIR / "database.db"
SESSION_PATH = BASE_DIR / "my_account"
LOG_PATH = BASE_DIR / "app.log"
ENV_PATH = BASE_DIR / ".env"
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
UPLOAD_TMP_DIR = BASE_DIR / "_uploads"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", maxsplit=1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(frozen=True)
class Settings:
    api_id: int | None
    api_hash: str
    phone: str
    password: str
    web_host: str
    web_port: int
    base_dir: Path = BASE_DIR
    media_dir: Path = MEDIA_DIR
    db_path: Path = DB_PATH
    session_path: Path = SESSION_PATH
    log_path: Path = LOG_PATH
    templates_dir: Path = TEMPLATES_DIR
    static_dir: Path = STATIC_DIR
    upload_tmp_dir: Path = UPLOAD_TMP_DIR

    @property
    def telegram_ready(self) -> bool:
        return bool(self.api_id and self.api_hash)


def get_settings() -> Settings:
    load_env_file(ENV_PATH)

    api_id_raw = os.getenv("TG_API_ID", "").strip()
    api_id = int(api_id_raw) if api_id_raw.isdigit() else None
    web_port_raw = os.getenv("WEB_PORT", "8000").strip()
    web_port = int(web_port_raw) if web_port_raw.isdigit() else 8000

    return Settings(
        api_id=api_id,
        api_hash=os.getenv("TG_API_HASH", "").strip(),
        phone=os.getenv("TG_PHONE", "").strip(),
        password=os.getenv("TG_PASSWORD", "").strip(),
        web_host=os.getenv("WEB_HOST", "127.0.0.1").strip() or "127.0.0.1",
        web_port=web_port,
    )

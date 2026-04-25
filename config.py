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
    base_dir: Path = BASE_DIR
    media_dir: Path = MEDIA_DIR
    db_path: Path = DB_PATH
    session_path: Path = SESSION_PATH
    log_path: Path = LOG_PATH

    @property
    def telegram_ready(self) -> bool:
        return bool(self.api_id and self.api_hash)


def get_settings() -> Settings:
    load_env_file(ENV_PATH)

    api_id_raw = os.getenv("TG_API_ID", "").strip()
    api_id = int(api_id_raw) if api_id_raw.isdigit() else None

    return Settings(
        api_id=api_id,
        api_hash=os.getenv("TG_API_HASH", "").strip(),
        phone=os.getenv("TG_PHONE", "").strip(),
        password=os.getenv("TG_PASSWORD", "").strip(),
    )

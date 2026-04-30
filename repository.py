import shutil
import sqlite3
import json
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


class Repository:
    def __init__(self, db_path: Path, media_dir: Path) -> None:
        self.db_path = db_path
        self.media_dir = media_dir
        self.media_dir.mkdir(exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    def init_db(self) -> None:
        with closing(self.connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    chat_ref TEXT NOT NULL UNIQUE,
                    topic_id INTEGER,
                    topic_title TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    text TEXT NOT NULL,
                    interval_days INTEGER NOT NULL DEFAULT 1,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ad_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ad_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS ad_targets (
                    ad_id INTEGER NOT NULL,
                    target_id INTEGER NOT NULL,
                    PRIMARY KEY (ad_id, target_id),
                    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE,
                    FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ad_id INTEGER NOT NULL,
                    time_of_day TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS publish_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ad_id INTEGER NOT NULL,
                    target_id INTEGER,
                    status TEXT NOT NULL,
                    message TEXT,
                    published_at TEXT NOT NULL,
                    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE,
                    FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS publish_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ad_id INTEGER NOT NULL,
                    run_type TEXT NOT NULL,
                    slot_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    not_before TEXT NOT NULL,
                    locked_at TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS publish_dedup (
                    ad_id INTEGER NOT NULL,
                    target_id INTEGER NOT NULL,
                    slot_key TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (ad_id, target_id, slot_key),
                    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE,
                    FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS system_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    payload TEXT,
                    created_at TEXT NOT NULL,
                    processed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS monitor_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    chat_ref TEXT NOT NULL UNIQUE,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS monitor_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    include_keywords TEXT NOT NULL DEFAULT '',
                    exclude_keywords TEXT NOT NULL DEFAULT '',
                    min_price INTEGER,
                    max_price INTEGER,
                    require_photo INTEGER NOT NULL DEFAULT 0,
                    notify_to TEXT NOT NULL DEFAULT 'me',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS monitor_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL,
                    rule_id INTEGER NOT NULL,
                    external_chat_id TEXT NOT NULL,
                    external_message_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    detected_price INTEGER,
                    has_photo INTEGER NOT NULL DEFAULT 0,
                    matched_keywords TEXT NOT NULL DEFAULT '',
                    notified INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (source_id) REFERENCES monitor_sources(id) ON DELETE CASCADE,
                    FOREIGN KEY (rule_id) REFERENCES monitor_rules(id) ON DELETE CASCADE,
                    UNIQUE(rule_id, external_chat_id, external_message_id)
                );

                CREATE TABLE IF NOT EXISTS monitor_rule_sources (
                    rule_id INTEGER NOT NULL,
                    source_id INTEGER NOT NULL,
                    PRIMARY KEY (rule_id, source_id),
                    FOREIGN KEY (rule_id) REFERENCES monitor_rules(id) ON DELETE CASCADE,
                    FOREIGN KEY (source_id) REFERENCES monitor_sources(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS monitor_test_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    rule_payload TEXT NOT NULL,
                    source_ids TEXT NOT NULL,
                    scan_limit INTEGER NOT NULL,
                    total_scanned INTEGER NOT NULL DEFAULT 0,
                    total_matches INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS monitor_test_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    source_id INTEGER NOT NULL,
                    source_name TEXT NOT NULL,
                    external_chat_id TEXT NOT NULL,
                    external_message_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    detected_price INTEGER,
                    has_photo INTEGER NOT NULL DEFAULT 0,
                    matched_keywords TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES monitor_test_runs(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_publish_log_published_at
                    ON publish_log (published_at DESC);
                CREATE INDEX IF NOT EXISTS idx_ad_targets_ad_target
                    ON ad_targets (ad_id, target_id);
                CREATE INDEX IF NOT EXISTS idx_schedules_ad_time
                    ON schedules (ad_id, time_of_day);
                CREATE INDEX IF NOT EXISTS idx_publish_jobs_status_not_before
                    ON publish_jobs (status, not_before);
                CREATE INDEX IF NOT EXISTS idx_publish_jobs_updated_at
                    ON publish_jobs (updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_system_commands_status_id
                    ON system_commands (status, id);
                CREATE INDEX IF NOT EXISTS idx_monitor_sources_active
                    ON monitor_sources (is_active, id);
                CREATE INDEX IF NOT EXISTS idx_monitor_rules_active
                    ON monitor_rules (is_active, id);
                CREATE INDEX IF NOT EXISTS idx_monitor_matches_created
                    ON monitor_matches (created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_monitor_rule_sources_rule
                    ON monitor_rule_sources (rule_id, source_id);
                CREATE INDEX IF NOT EXISTS idx_monitor_test_runs_status
                    ON monitor_test_runs (status, id);
                CREATE INDEX IF NOT EXISTS idx_monitor_test_results_run
                    ON monitor_test_results (run_id, id);
                """
            )
            self._ensure_targets_columns(conn)
            conn.commit()

    def _ensure_targets_columns(self, conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(targets)").fetchall()
        }
        if "topic_id" not in columns:
            conn.execute("ALTER TABLE targets ADD COLUMN topic_id INTEGER")
        if "topic_title" not in columns:
            conn.execute("ALTER TABLE targets ADD COLUMN topic_title TEXT")

    def list_targets(self) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT id, name, chat_ref, topic_id, topic_title, is_active, created_at, updated_at
                FROM targets
                ORDER BY name COLLATE NOCASE, id
                """
            ).fetchall()

    def get_target(self, target_id: int) -> sqlite3.Row | None:
        with closing(self.connect()) as conn:
            return conn.execute(
                "SELECT * FROM targets WHERE id = ?",
                (target_id,),
            ).fetchone()

    def save_target(
        self,
        target_id: int | None,
        name: str,
        chat_ref: str,
        topic_id: int | None,
        topic_title: str,
        is_active: bool,
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            if target_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO targets (name, chat_ref, topic_id, topic_title, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (name, chat_ref, topic_id, topic_title, int(is_active), now, now),
                )
                conn.commit()
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE targets
                SET name = ?, chat_ref = ?, topic_id = ?, topic_title = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (name, chat_ref, topic_id, topic_title, int(is_active), now, target_id),
            )
            conn.commit()
            return target_id

    def delete_target(self, target_id: int) -> None:
        with closing(self.connect()) as conn:
            conn.execute("DELETE FROM targets WHERE id = ?", (target_id,))
            conn.commit()

    def list_ads(self) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    a.id,
                    a.title,
                    a.interval_days,
                    a.is_active,
                    COUNT(DISTINCT at.target_id) AS target_count,
                    COUNT(DISTINCT s.id) AS schedule_count
                FROM ads a
                LEFT JOIN ad_targets at ON at.ad_id = a.id
                LEFT JOIN schedules s ON s.ad_id = a.id AND s.is_active = 1
                GROUP BY a.id, a.title, a.interval_days, a.is_active
                ORDER BY a.updated_at DESC, a.id DESC
                """
            ).fetchall()

    def get_ad(self, ad_id: int) -> dict | None:
        with closing(self.connect()) as conn:
            ad = conn.execute(
                "SELECT * FROM ads WHERE id = ?",
                (ad_id,),
            ).fetchone()
            if not ad:
                return None

            media = conn.execute(
                """
                SELECT file_path
                FROM ad_media
                WHERE ad_id = ?
                ORDER BY sort_order, id
                """,
                (ad_id,),
            ).fetchall()
            target_rows = conn.execute(
                """
                SELECT t.id, t.name, t.chat_ref, t.topic_id, t.topic_title
                FROM ad_targets at
                JOIN targets t ON t.id = at.target_id
                WHERE at.ad_id = ?
                ORDER BY t.name COLLATE NOCASE, t.id
                """,
                (ad_id,),
            ).fetchall()
            schedule_rows = conn.execute(
                """
                SELECT id, time_of_day, is_active
                FROM schedules
                WHERE ad_id = ?
                ORDER BY time_of_day
                """,
                (ad_id,),
            ).fetchall()

        return {
            "id": ad["id"],
            "title": ad["title"],
            "text": ad["text"],
            "interval_days": ad["interval_days"],
            "is_active": bool(ad["is_active"]),
            "media_paths": [row["file_path"] for row in media],
            "targets": [dict(row) for row in target_rows],
            "target_ids": [row["id"] for row in target_rows],
            "times": [row["time_of_day"] for row in schedule_rows if row["is_active"]],
        }

    def save_ad(
        self,
        ad_id: int | None,
        title: str,
        text: str,
        interval_days: int,
        is_active: bool,
        media_sources: list[str],
        target_ids: list[int],
        times: list[str],
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            if ad_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO ads (title, text, interval_days, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (title, text, interval_days, int(is_active), now, now),
                )
                ad_id = int(cursor.lastrowid)
            else:
                conn.execute(
                    """
                    UPDATE ads
                    SET title = ?, text = ?, interval_days = ?, is_active = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (title, text, interval_days, int(is_active), now, ad_id),
                )
                conn.execute("DELETE FROM ad_targets WHERE ad_id = ?", (ad_id,))
                conn.execute("DELETE FROM schedules WHERE ad_id = ?", (ad_id,))
                conn.execute("DELETE FROM ad_media WHERE ad_id = ?", (ad_id,))

            for target_id in target_ids:
                conn.execute(
                    "INSERT INTO ad_targets (ad_id, target_id) VALUES (?, ?)",
                    (ad_id, target_id),
                )

            for raw_time in times:
                conn.execute(
                    "INSERT INTO schedules (ad_id, time_of_day, is_active) VALUES (?, ?, 1)",
                    (ad_id, raw_time),
                )

            stored_paths = self._store_media_files(ad_id, media_sources)
            for index, file_path in enumerate(stored_paths):
                conn.execute(
                    """
                    INSERT INTO ad_media (ad_id, file_path, sort_order)
                    VALUES (?, ?, ?)
                    """,
                    (ad_id, file_path, index),
                )

            conn.commit()
            return ad_id

    def delete_ad(self, ad_id: int) -> None:
        with closing(self.connect()) as conn:
            conn.execute("DELETE FROM ads WHERE id = ?", (ad_id,))
            conn.commit()

        ad_media_dir = self.media_dir / str(ad_id)
        if ad_media_dir.exists():
            shutil.rmtree(ad_media_dir, ignore_errors=True)

    def list_active_schedules(self) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    a.id AS ad_id,
                    a.interval_days,
                    s.time_of_day
                FROM ads a
                JOIN schedules s ON s.ad_id = a.id
                WHERE a.is_active = 1 AND s.is_active = 1
                ORDER BY a.id, s.time_of_day
                """
            ).fetchall()

    def get_publish_payload(self, ad_id: int) -> dict | None:
        with closing(self.connect()) as conn:
            ad = conn.execute(
                """
                SELECT id, title, text, interval_days, is_active
                FROM ads
                WHERE id = ?
                """,
                (ad_id,),
            ).fetchone()
            if not ad:
                return None

            media = conn.execute(
                """
                SELECT file_path
                FROM ad_media
                WHERE ad_id = ?
                ORDER BY sort_order, id
                """,
                (ad_id,),
            ).fetchall()
            targets = conn.execute(
                """
                SELECT t.id, t.name, t.chat_ref, t.topic_id, t.topic_title
                FROM ad_targets at
                JOIN targets t ON t.id = at.target_id
                WHERE at.ad_id = ? AND t.is_active = 1
                ORDER BY t.name COLLATE NOCASE, t.id
                """,
                (ad_id,),
            ).fetchall()

        return {
            "id": ad["id"],
            "title": ad["title"],
            "text": ad["text"],
            "is_active": bool(ad["is_active"]),
            "media_paths": [row["file_path"] for row in media],
            "targets": [dict(row) for row in targets],
        }

    def add_publish_log(
        self,
        ad_id: int,
        target_id: int | None,
        status: str,
        message: str,
    ) -> None:
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO publish_log (ad_id, target_id, status, message, published_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    ad_id,
                    target_id,
                    status,
                    message,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
            conn.commit()

    def list_publish_logs(self, limit: int = 200) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    pl.id,
                    pl.published_at,
                    pl.status,
                    pl.message,
                    a.title AS ad_title,
                    t.name AS target_name,
                    t.chat_ref AS target_chat_ref
                FROM publish_log pl
                JOIN ads a ON a.id = pl.ad_id
                LEFT JOIN targets t ON t.id = pl.target_id
                ORDER BY pl.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def enqueue_publish_job(
        self,
        ad_id: int,
        run_type: str,
        slot_key: str,
        not_before: datetime | None = None,
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        not_before_value = (not_before or datetime.now()).isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO publish_jobs (ad_id, run_type, slot_key, status, not_before, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?)
                """,
                (ad_id, run_type, slot_key, not_before_value, now, now),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def acquire_next_publish_job(self) -> dict[str, Any] | None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT id, ad_id, run_type, slot_key, attempts, max_attempts, not_before
                FROM publish_jobs
                WHERE status IN ('pending', 'retry')
                  AND not_before <= ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if not row:
                return None

            updated = conn.execute(
                """
                UPDATE publish_jobs
                SET status = 'running',
                    locked_at = ?,
                    started_at = COALESCE(started_at, ?),
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id = ?
                  AND status IN ('pending', 'retry')
                """,
                (now, now, now, row["id"]),
            )
            if updated.rowcount == 0:
                conn.commit()
                return None

            conn.commit()
            return dict(row)

    def complete_publish_job(self, job_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                UPDATE publish_jobs
                SET status = 'done', finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, job_id),
            )
            conn.commit()

    def fail_publish_job(self, job_id: int, last_error: str, requeue: bool) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        next_status = "retry" if requeue else "failed"
        next_attempt_at = datetime.now()
        if requeue:
            next_attempt_at = datetime.now() + timedelta(minutes=1)
        next_attempt_at_value = next_attempt_at.isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                UPDATE publish_jobs
                SET status = ?,
                    last_error = ?,
                    not_before = CASE WHEN ? = 'retry' THEN ? ELSE not_before END,
                    finished_at = CASE WHEN ? = 'failed' THEN ? ELSE finished_at END,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_status, last_error, next_status, next_attempt_at_value, next_status, now, now, job_id),
            )
            conn.commit()

    def should_retry_publish_job(self, job_id: int) -> bool:
        with closing(self.connect()) as conn:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM publish_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            if not row:
                return False
            return int(row["attempts"]) < int(row["max_attempts"])

    def reserve_publish_slot(self, ad_id: int, target_id: int, slot_key: str) -> bool:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO publish_dedup (ad_id, target_id, slot_key, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (ad_id, target_id, slot_key, now),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def enqueue_system_command(self, command: str, payload: str = "") -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO system_commands (command, payload, status, created_at)
                VALUES (?, ?, 'pending', ?)
                """,
                (command, payload, now),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def acquire_next_system_command(self) -> dict[str, Any] | None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT id, command, payload
                FROM system_commands
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            updated = conn.execute(
                """
                UPDATE system_commands
                SET status = 'processing'
                WHERE id = ? AND status = 'pending'
                """,
                (row["id"],),
            )
            if updated.rowcount == 0:
                conn.commit()
                return None
            conn.commit()
            return dict(row)

    def complete_system_command(self, command_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                UPDATE system_commands
                SET status = 'done', processed_at = ?
                WHERE id = ?
                """,
                (now, command_id),
            )
            conn.commit()

    def list_monitor_sources(self) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT id, name, chat_ref, is_active, created_at, updated_at
                FROM monitor_sources
                ORDER BY updated_at DESC, id DESC
                """
            ).fetchall()

    def list_monitor_sources_by_ids(self, source_ids: list[int]) -> list[dict[str, Any]]:
        if not source_ids:
            return []
        placeholders = ",".join("?" for _ in source_ids)
        with closing(self.connect()) as conn:
            rows = conn.execute(
                f"""
                SELECT id, name, chat_ref, is_active
                FROM monitor_sources
                WHERE id IN ({placeholders})
                ORDER BY id
                """,
                tuple(source_ids),
            ).fetchall()
            return [dict(row) for row in rows]

    def save_monitor_source(
        self,
        source_id: int | None,
        name: str,
        chat_ref: str,
        is_active: bool,
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            if source_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO monitor_sources (name, chat_ref, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (name, chat_ref, int(is_active), now, now),
                )
                conn.commit()
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE monitor_sources
                SET name = ?, chat_ref = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (name, chat_ref, int(is_active), now, source_id),
            )
            conn.commit()
            return source_id

    def delete_monitor_source(self, source_id: int) -> None:
        with closing(self.connect()) as conn:
            conn.execute("DELETE FROM monitor_sources WHERE id = ?", (source_id,))
            conn.commit()

    def list_monitor_rules(self) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    id, name, include_keywords, exclude_keywords, min_price, max_price,
                    require_photo, notify_to, is_active, created_at, updated_at
                FROM monitor_rules
                ORDER BY updated_at DESC, id DESC
                """
            ).fetchall()

    def save_monitor_rule(
        self,
        rule_id: int | None,
        name: str,
        include_keywords: str,
        exclude_keywords: str,
        min_price: int | None,
        max_price: int | None,
        require_photo: bool,
        notify_to: str,
        is_active: bool,
        source_ids: list[int],
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            if rule_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO monitor_rules (
                        name, include_keywords, exclude_keywords, min_price, max_price,
                        require_photo, notify_to, is_active, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        include_keywords,
                        exclude_keywords,
                        min_price,
                        max_price,
                        int(require_photo),
                        notify_to,
                        int(is_active),
                        now,
                        now,
                    ),
                )
                saved_rule_id = int(cursor.lastrowid)
                self._replace_monitor_rule_sources(conn, saved_rule_id, source_ids)
                conn.commit()
                return saved_rule_id

            conn.execute(
                """
                UPDATE monitor_rules
                SET
                    name = ?, include_keywords = ?, exclude_keywords = ?, min_price = ?, max_price = ?,
                    require_photo = ?, notify_to = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    include_keywords,
                    exclude_keywords,
                    min_price,
                    max_price,
                    int(require_photo),
                    notify_to,
                    int(is_active),
                    now,
                    rule_id,
                ),
            )
            self._replace_monitor_rule_sources(conn, rule_id, source_ids)
            conn.commit()
            return rule_id

    def delete_monitor_rule(self, rule_id: int) -> None:
        with closing(self.connect()) as conn:
            conn.execute("DELETE FROM monitor_rules WHERE id = ?", (rule_id,))
            conn.commit()

    def list_monitor_matches(self, limit: int = 200) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    mm.id, mm.created_at, mm.message_text, mm.detected_price, mm.has_photo, mm.matched_keywords,
                    mm.notified, mm.external_chat_id, mm.external_message_id,
                    ms.name AS source_name, ms.chat_ref AS source_chat_ref,
                    mr.name AS rule_name
                FROM monitor_matches mm
                JOIN monitor_sources ms ON ms.id = mm.source_id
                JOIN monitor_rules mr ON mr.id = mm.rule_id
                ORDER BY mm.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def list_active_monitoring_sources(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as conn:
            rows = conn.execute(
                """
                SELECT id, name, chat_ref
                FROM monitor_sources
                WHERE is_active = 1
                ORDER BY id
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_active_monitoring_rules(self) -> list[dict[str, Any]]:
        with closing(self.connect()) as conn:
            rows = conn.execute(
                """
                SELECT
                    id, name, include_keywords, exclude_keywords, min_price, max_price,
                    require_photo, notify_to
                FROM monitor_rules
                WHERE is_active = 1
                ORDER BY id
                """
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                rule_data = dict(row)
                rule_data["source_ids"] = self._get_monitor_rule_sources_for_conn(conn, int(row["id"]))
                result.append(rule_data)
            return result

    def get_monitor_rule_source_ids(self, rule_id: int) -> list[int]:
        with closing(self.connect()) as conn:
            return self._get_monitor_rule_sources_for_conn(conn, rule_id)

    def add_monitor_match(
        self,
        source_id: int,
        rule_id: int,
        external_chat_id: str,
        external_message_id: int,
        message_text: str,
        detected_price: int | None,
        has_photo: bool,
        matched_keywords: str,
        notified: bool,
    ) -> bool:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO monitor_matches (
                        source_id, rule_id, external_chat_id, external_message_id, message_text,
                        detected_price, has_photo, matched_keywords, notified, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_id,
                        rule_id,
                        external_chat_id,
                        external_message_id,
                        message_text,
                        detected_price,
                        int(has_photo),
                        matched_keywords,
                        int(notified),
                        now,
                    ),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def _replace_monitor_rule_sources(self, conn: sqlite3.Connection, rule_id: int, source_ids: list[int]) -> None:
        conn.execute("DELETE FROM monitor_rule_sources WHERE rule_id = ?", (rule_id,))
        for source_id in source_ids:
            conn.execute(
                "INSERT INTO monitor_rule_sources (rule_id, source_id) VALUES (?, ?)",
                (rule_id, source_id),
            )

    def _get_monitor_rule_sources_for_conn(self, conn: sqlite3.Connection, rule_id: int) -> list[int]:
        rows = conn.execute(
            """
            SELECT source_id
            FROM monitor_rule_sources
            WHERE rule_id = ?
            ORDER BY source_id
            """,
            (rule_id,),
        ).fetchall()
        return [int(row["source_id"]) for row in rows]

    def create_monitor_test_run(self, rule_payload: dict[str, Any], source_ids: list[int], scan_limit: int) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO monitor_test_runs (status, rule_payload, source_ids, scan_limit, created_at, updated_at)
                VALUES ('pending', ?, ?, ?, ?, ?)
                """,
                (json.dumps(rule_payload, ensure_ascii=False), json.dumps(source_ids), scan_limit, now, now),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def start_monitor_test_run(self, run_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                UPDATE monitor_test_runs
                SET status = 'running', updated_at = ?, error_message = NULL, total_scanned = 0, total_matches = 0
                WHERE id = ?
                """,
                (now, run_id),
            )
            conn.execute("DELETE FROM monitor_test_results WHERE run_id = ?", (run_id,))
            conn.commit()

    def add_monitor_test_result(
        self,
        run_id: int,
        source_id: int,
        source_name: str,
        external_chat_id: str,
        external_message_id: int,
        message_text: str,
        detected_price: int | None,
        has_photo: bool,
        matched_keywords: str,
    ) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                INSERT INTO monitor_test_results (
                    run_id, source_id, source_name, external_chat_id, external_message_id,
                    message_text, detected_price, has_photo, matched_keywords, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source_id,
                    source_name,
                    external_chat_id,
                    external_message_id,
                    message_text,
                    detected_price,
                    int(has_photo),
                    matched_keywords,
                    now,
                ),
            )
            conn.commit()

    def finish_monitor_test_run(self, run_id: int, total_scanned: int, total_matches: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                UPDATE monitor_test_runs
                SET status = 'done', total_scanned = ?, total_matches = ?, updated_at = ?
                WHERE id = ?
                """,
                (total_scanned, total_matches, now, run_id),
            )
            conn.commit()

    def fail_monitor_test_run(self, run_id: int, error_message: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            conn.execute(
                """
                UPDATE monitor_test_runs
                SET status = 'failed', error_message = ?, updated_at = ?
                WHERE id = ?
                """,
                (error_message, now, run_id),
            )
            conn.commit()

    def get_monitor_test_run(self, run_id: int) -> sqlite3.Row | None:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT id, status, scan_limit, total_scanned, total_matches, error_message, created_at, updated_at
                FROM monitor_test_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()

    def get_monitor_test_payload(self, run_id: int) -> dict[str, Any] | None:
        with closing(self.connect()) as conn:
            row = conn.execute(
                """
                SELECT rule_payload, source_ids, scan_limit
                FROM monitor_test_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "rule_payload": json.loads(row["rule_payload"]),
                "source_ids": json.loads(row["source_ids"]),
                "scan_limit": int(row["scan_limit"]),
            }

    def list_monitor_test_results(self, run_id: int, limit: int = 200) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT
                    id, run_id, source_id, source_name, external_chat_id, external_message_id,
                    message_text, detected_price, has_photo, matched_keywords, created_at
                FROM monitor_test_results
                WHERE run_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()

    def _store_media_files(self, ad_id: int, media_sources: list[str]) -> list[str]:
        target_dir = self.media_dir / str(ad_id)
        temp_dir = self.media_dir / f".tmp_{ad_id}"

        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)

        stored_paths: list[str] = []
        for index, raw_source in enumerate(media_sources):
            source = Path(raw_source).expanduser()
            if not source.is_absolute():
                source = self.db_path.parent / source
            source = source.resolve()

            if not source.exists():
                raise FileNotFoundError(f"Media file not found: {source}")

            destination = temp_dir / f"{index + 1:02d}_{source.name}"
            if source != destination:
                shutil.copy2(source, destination)
            stored_paths.append(str(destination.relative_to(self.db_path.parent)))

        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        temp_dir.replace(target_dir)

        return [
            str((target_dir / Path(path).name).relative_to(self.db_path.parent))
            for path in stored_paths
        ]

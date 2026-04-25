import shutil
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path


class Repository:
    def __init__(self, db_path: Path, media_dir: Path) -> None:
        self.db_path = db_path
        self.media_dir = media_dir
        self.media_dir.mkdir(exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def init_db(self) -> None:
        with closing(self.connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    chat_ref TEXT NOT NULL UNIQUE,
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
                """
            )
            conn.commit()

    def list_targets(self) -> list[sqlite3.Row]:
        with closing(self.connect()) as conn:
            return conn.execute(
                """
                SELECT id, name, chat_ref, is_active, created_at, updated_at
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
        is_active: bool,
    ) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with closing(self.connect()) as conn:
            if target_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO targets (name, chat_ref, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (name, chat_ref, int(is_active), now, now),
                )
                conn.commit()
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE targets
                SET name = ?, chat_ref = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (name, chat_ref, int(is_active), now, target_id),
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
                SELECT t.id, t.name, t.chat_ref
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
                SELECT t.id, t.name, t.chat_ref
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

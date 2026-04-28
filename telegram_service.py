import asyncio
import logging
import random
import sqlite3
import threading
from concurrent.futures import Future
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient, functions


class TelegramService:
    def __init__(self, settings, repository, logger: logging.Logger) -> None:
        self.settings = settings
        self.repository = repository
        self.logger = logger
        self.status = "Не запущено"
        self._client: TelegramClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._scheduler: AsyncIOScheduler | None = None
        self._thread: threading.Thread | None = None
        self._job_loop_task: asyncio.Task | None = None
        self._command_loop_task: asyncio.Task | None = None
        self._running_lock = asyncio.Lock()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run_background_loop,
            name="telegram-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if not self._loop:
            return
        future = asyncio.run_coroutine_threadsafe(self._async_stop(), self._loop)
        future.result(timeout=10)

    def get_status(self) -> str:
        return self.status

    def refresh_scheduler(self) -> None:
        self.repository.enqueue_system_command("reload_scheduler")
        if not self._loop:
            return
        asyncio.run_coroutine_threadsafe(self._process_system_commands(), self._loop)

    def publish_now(self, ad_id: int) -> Future:
        slot_key = f"manual:{ad_id}:{datetime.now().isoformat(timespec='seconds')}"
        self.repository.enqueue_publish_job(ad_id, "manual", slot_key)
        if self._loop:
            return asyncio.run_coroutine_threadsafe(self._process_publish_jobs(), self._loop)

        future: Future = Future()
        future.set_result(None)
        return future

    def list_forum_topics(self, chat_ref: str) -> list[dict]:
        if not self._loop:
            raise RuntimeError("Фоновый процесс Telegram еще не запущен.")
        future = asyncio.run_coroutine_threadsafe(self._list_forum_topics(chat_ref), self._loop)
        return future.result(timeout=20)

    def _run_background_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._async_start())
        self._loop.run_forever()

    async def _async_start(self) -> None:
        if not self.settings.telegram_ready:
            self.status = "Не заданы TG_API_ID / TG_API_HASH"
            self.logger.warning(
                "Не заданы учетные данные Telegram. Создайте .env или укажите переменные окружения."
            )
            return

        try:
            self.status = "Подключение к Telegram..."
            self._client = TelegramClient(
                str(self.settings.session_path),
                self.settings.api_id,
                self.settings.api_hash,
            )
            await self._client.connect()

            if not await self._client.is_user_authorized():
                self.status = "Требуется авторизация"
                self.logger.warning(
                    "Сессия Telegram не авторизована. При первом запуске приложение может запросить код или пароль в консоли."
                )
                await self._client.start(
                    phone=self.settings.phone or None,
                    password=self.settings.password or None,
                )

            self.status = "Telegram подключен"
            self.logger.info("Аккаунт Telegram подключен.")

            self._scheduler = AsyncIOScheduler()
            await self._reload_scheduler()
            self._scheduler.start()
            self._job_loop_task = self._loop.create_task(self._run_job_loop())
            self._command_loop_task = self._loop.create_task(self._run_command_loop())
            self.status = "Расписание запущено"
            self.logger.info("Планировщик запущен.")
        except Exception as exc:
            if self._is_session_locked_error(exc):
                self.status = "Сессия Telegram занята другим экземпляром приложения"
                self.logger.error(
                    "Сессия Telegram заблокирована другим процессом. "
                    "Закройте другие экземпляры tg_poster.exe / python, которые используют тот же my_account.session."
                )
                return
            self.status = f"Ошибка запуска: {exc}"
            self.logger.exception("Ошибка запуска Telegram: %s", exc)

    async def _async_stop(self) -> None:
        try:
            if self._scheduler:
                self._scheduler.shutdown(wait=False)
                self._scheduler = None
            if self._job_loop_task:
                self._job_loop_task.cancel()
                self._job_loop_task = None
            if self._command_loop_task:
                self._command_loop_task.cancel()
                self._command_loop_task = None

            if self._client:
                await self._client.disconnect()
                self._client = None
        finally:
            self.status = "Остановлено"
            if self._loop and self._loop.is_running():
                self._loop.call_soon(self._loop.stop)

    async def _reload_scheduler(self) -> None:
        if not self._scheduler:
            return

        for job in self._scheduler.get_jobs():
            job.remove()

        for row in self.repository.list_active_schedules():
            next_run = self._next_run_datetime(row["time_of_day"])
            self._scheduler.add_job(
                self._enqueue_scheduled_publish,
                trigger="interval",
                days=row["interval_days"],
                start_date=next_run,
                args=[row["ad_id"], row["time_of_day"]],
                id=f"ad-{row['ad_id']}-{row['time_of_day']}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
            )

        self.logger.info("Расписание обновлено, активных задач: %s.", len(self._scheduler.get_jobs()))

    def _next_run_datetime(self, time_of_day: str) -> datetime:
        hour_text, minute_text = time_of_day.split(":", maxsplit=1)
        candidate = datetime.now().replace(
            hour=int(hour_text),
            minute=int(minute_text),
            second=0,
            microsecond=0,
        )
        if candidate <= datetime.now():
            candidate += timedelta(days=1)
        return candidate

    async def _enqueue_scheduled_publish(self, ad_id: int, time_of_day: str) -> None:
        slot_key = f"schedule:{ad_id}:{time_of_day}:{datetime.now().strftime('%Y-%m-%d')}"
        self.repository.enqueue_publish_job(ad_id, "schedule", slot_key)
        await self._process_publish_jobs()

    async def _run_job_loop(self) -> None:
        while True:
            try:
                await self._process_publish_jobs()
            except Exception as exc:
                self.logger.exception("Ошибка обработки очереди публикаций: %s", exc)
            await asyncio.sleep(2)

    async def _run_command_loop(self) -> None:
        while True:
            try:
                await self._process_system_commands()
            except Exception as exc:
                self.logger.exception("Ошибка обработки системных команд: %s", exc)
            await asyncio.sleep(2)

    async def _process_system_commands(self) -> None:
        while True:
            command = self.repository.acquire_next_system_command()
            if not command:
                return
            try:
                if command["command"] == "reload_scheduler":
                    await self._reload_scheduler()
            finally:
                self.repository.complete_system_command(command["id"])

    async def _process_publish_jobs(self) -> None:
        if not self._client:
            return

        async with self._running_lock:
            while True:
                job = self.repository.acquire_next_publish_job()
                if not job:
                    return

                try:
                    await self._publish_ad(job["ad_id"], job["slot_key"], manual=(job["run_type"] == "manual"))
                    self.repository.complete_publish_job(job["id"])
                except Exception as exc:
                    error_text = str(exc)
                    should_retry = self.repository.should_retry_publish_job(job["id"])
                    self.repository.fail_publish_job(job["id"], error_text, should_retry)
                    self.logger.exception(
                        "Публикация ad_id=%s (job_id=%s) завершилась ошибкой: %s",
                        job["ad_id"],
                        job["id"],
                        exc,
                    )

    async def _publish_ad(self, ad_id: int, slot_key: str, manual: bool = False) -> None:
        if not self._client:
            self.logger.warning("Публикация объявления %s пропущена: клиент Telegram не готов.", ad_id)
            return

        payload = self.repository.get_publish_payload(ad_id)
        if not payload:
            self.logger.warning("Публикация пропущена: объявление %s не найдено.", ad_id)
            return
        if not payload["is_active"]:
            self.logger.info("Публикация объявления %s пропущена: объявление неактивно.", ad_id)
            return
        if not payload["targets"]:
            self.logger.warning("Публикация объявления %s пропущена: нет активных целей.", ad_id)
            return

        media_files = [self._resolve_media_path(path) for path in payload["media_paths"]]
        media_files = [str(path) for path in media_files if path.exists()]

        publish_mode = "ручную" if manual else "по расписанию"
        self.logger.info("Запуск публикации %s для объявления %s.", publish_mode, ad_id)

        for target in payload["targets"]:
            try:
                if not self.repository.reserve_publish_slot(ad_id, target["id"], slot_key):
                    self.logger.info(
                        "Публикация ad_id=%s target_id=%s пропущена: слот %s уже обработан.",
                        ad_id,
                        target["id"],
                        slot_key,
                    )
                    continue

                topic_id = target.get("topic_id")
                if len(media_files) > 1:
                    await self._client.send_file(
                        target["chat_ref"],
                        media_files,
                        caption=payload["text"],
                        reply_to=topic_id,
                    )
                elif len(media_files) == 1:
                    await self._client.send_file(
                        target["chat_ref"],
                        media_files[0],
                        caption=payload["text"],
                        reply_to=topic_id,
                    )
                else:
                    await self._client.send_message(
                        target["chat_ref"],
                        payload["text"],
                        reply_to=topic_id,
                    )

                destination_label = target["chat_ref"]
                if topic_id and target.get("topic_title"):
                    destination_label = f"{destination_label} -> {target['topic_title']}"
                elif topic_id:
                    destination_label = f"{destination_label} -> topic {topic_id}"

                self.repository.add_publish_log(
                    ad_id,
                    target["id"],
                    "успех",
                    f"Опубликовано в {destination_label}",
                )
                self.logger.info("Объявление %s опубликовано в %s.", ad_id, destination_label)

                pause_seconds = random.randint(30, 60)
                await asyncio.sleep(pause_seconds)
            except Exception as exc:
                destination_label = target["chat_ref"]
                if target.get("topic_title"):
                    destination_label = f"{destination_label} -> {target['topic_title']}"
                elif target.get("topic_id"):
                    destination_label = f"{destination_label} -> topic {target['topic_id']}"
                error_message = f"Не удалось опубликовать в {destination_label}: {exc}"
                self.repository.add_publish_log(ad_id, target["id"], "ошибка", error_message)
                self.logger.exception(error_message)

    async def _list_forum_topics(self, chat_ref: str) -> list[dict]:
        if not self._client:
            raise RuntimeError("Клиент Telegram не готов.")

        result = await self._client(
            functions.messages.GetForumTopicsRequest(
                peer=chat_ref,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=100,
            )
        )

        topics: list[dict] = []
        for topic in result.topics:
            title = getattr(topic, "title", None)
            if not title:
                continue
            topics.append({"id": topic.id, "title": title})

        return topics

    def _resolve_media_path(self, relative_path: str) -> Path:
        media_path = Path(relative_path)
        if not media_path.is_absolute():
            media_path = self.settings.base_dir / media_path
        return media_path

    def _is_session_locked_error(self, exc: Exception) -> bool:
        if isinstance(exc, sqlite3.OperationalError):
            return "database is locked" in str(exc).lower()

        current = exc
        while current:
            if isinstance(current, sqlite3.OperationalError) and "database is locked" in str(current).lower():
                return True
            current = current.__cause__ or current.__context__

        return False

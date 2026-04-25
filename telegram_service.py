import asyncio
import logging
import random
import threading
from concurrent.futures import Future
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient


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

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run_background_loop,
            name="telegram-worker",
            daemon=True,
        )
        self._thread.start()

    def get_status(self) -> str:
        return self.status

    def refresh_scheduler(self) -> None:
        if not self._loop:
            return
        asyncio.run_coroutine_threadsafe(self._reload_scheduler(), self._loop)

    def publish_now(self, ad_id: int) -> Future:
        if not self._loop:
            raise RuntimeError("Фоновый процесс Telegram еще не запущен.")
        return asyncio.run_coroutine_threadsafe(
            self._publish_ad(ad_id, manual=True),
            self._loop,
        )

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
            self.status = "Расписание запущено"
            self.logger.info("Планировщик запущен.")
        except Exception as exc:
            self.status = f"Ошибка запуска: {exc}"
            self.logger.exception("Ошибка запуска Telegram: %s", exc)

    async def _reload_scheduler(self) -> None:
        if not self._scheduler:
            return

        for job in self._scheduler.get_jobs():
            job.remove()

        for row in self.repository.list_active_schedules():
            next_run = self._next_run_datetime(row["time_of_day"])
            self._scheduler.add_job(
                self._publish_ad,
                trigger="interval",
                days=row["interval_days"],
                start_date=next_run,
                args=[row["ad_id"]],
                id=f"ad-{row['ad_id']}-{row['time_of_day']}",
                replace_existing=True,
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

    async def _publish_ad(self, ad_id: int, manual: bool = False) -> None:
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
                if len(media_files) > 1:
                    await self._client.send_file(
                        target["chat_ref"],
                        media_files,
                        caption=payload["text"],
                    )
                elif len(media_files) == 1:
                    await self._client.send_file(
                        target["chat_ref"],
                        media_files[0],
                        caption=payload["text"],
                    )
                else:
                    await self._client.send_message(target["chat_ref"], payload["text"])

                self.repository.add_publish_log(
                    ad_id,
                    target["id"],
                    "успех",
                    f"Опубликовано в {target['chat_ref']}",
                )
                self.logger.info("Объявление %s опубликовано в %s.", ad_id, target["chat_ref"])

                pause_seconds = random.randint(30, 60)
                await asyncio.sleep(pause_seconds)
            except Exception as exc:
                error_message = f"Не удалось опубликовать в {target['chat_ref']}: {exc}"
                self.repository.add_publish_log(ad_id, target["id"], "ошибка", error_message)
                self.logger.exception(error_message)

    def _resolve_media_path(self, relative_path: str) -> Path:
        media_path = Path(relative_path)
        if not media_path.is_absolute():
            media_path = self.settings.base_dir / media_path
        return media_path

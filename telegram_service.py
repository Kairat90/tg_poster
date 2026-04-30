import asyncio
import json
import logging
import random
import re
import sqlite3
import threading
from concurrent.futures import Future
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient, events, functions


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
        self._monitor_sources: list[dict] = []
        self._monitor_rules: list[dict] = []

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
        self.repository.enqueue_system_command("reload_monitoring")
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
            await self._reload_monitoring()
            self._register_monitor_handlers()
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
                if command["command"] == "reload_monitoring":
                    await self._reload_monitoring()
                if command["command"] == "run_monitor_test":
                    await self._run_monitor_test(command.get("payload", ""))
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

    async def _reload_monitoring(self) -> None:
        self._monitor_sources = self.repository.list_active_monitoring_sources()
        self._monitor_rules = self.repository.list_active_monitoring_rules()
        self.logger.info(
            "Мониторинг обновлен: источников=%s, правил=%s.",
            len(self._monitor_sources),
            len(self._monitor_rules),
        )

    def _register_monitor_handlers(self) -> None:
        if not self._client:
            return

        @self._client.on(events.NewMessage(incoming=True))
        async def monitor_incoming_message(event) -> None:
            await self._handle_monitoring_event(event)

    async def _handle_monitoring_event(self, event) -> None:
        if not self._monitor_sources or not self._monitor_rules:
            return

        chat = await event.get_chat()
        chat_id = str(event.chat_id or "")
        username = getattr(chat, "username", None)
        username_value = f"@{username.lower()}" if username else ""
        source = self._find_monitor_source(chat_id, username_value)
        if not source:
            return

        text = (event.raw_text or "").strip()
        if not text and not event.photo:
            return

        has_photo = bool(event.photo or event.media)
        detected_price = self._extract_price(text)
        for rule in self._monitor_rules:
            matched, matched_keywords = self._match_monitor_rule(
                rule,
                source["id"],
                text,
                detected_price,
                has_photo,
            )
            if not matched:
                continue

            notification_sent = await self._notify_monitor_match(
                source,
                rule,
                text,
                detected_price,
                has_photo,
                matched_keywords,
                chat_id,
                event.id,
            )
            self.repository.add_monitor_match(
                source_id=source["id"],
                rule_id=rule["id"],
                external_chat_id=chat_id,
                external_message_id=int(event.id),
                message_text=text[:4000],
                detected_price=detected_price,
                has_photo=has_photo,
                matched_keywords=",".join(matched_keywords),
                notified=notification_sent,
            )

    def _find_monitor_source(self, chat_id: str, username: str) -> dict | None:
        for source in self._monitor_sources:
            source_chat_ref = source["chat_ref"].strip().lower()
            if source_chat_ref.startswith("https://t.me/"):
                source_chat_ref = "@" + source_chat_ref.split("/")[-1]
            if source_chat_ref == chat_id or (username and source_chat_ref == username):
                return source
        return None

    def _extract_price(self, text: str) -> int | None:
        if not text:
            return None
        lowered = text.lower()

        # 1) Самый надежный сигнал - явная метка "цена/стоимость/за".
        labeled_pattern = re.compile(
            r"(?:цена|стоимость|продаю\s+за|отдам\s+за|за)\s*[:\-]?\s*"
            r"(\d+(?:[ \.,]\d{3})*)\s*(тыс|к|k|млн|m|₸|тг|тенге)?",
            re.IGNORECASE,
        )
        labeled_prices = [self._parse_price_candidate(m.group(1), m.group(2)) for m in labeled_pattern.finditer(lowered)]
        labeled_prices = [value for value in labeled_prices if value is not None]
        if labeled_prices:
            return labeled_prices[-1]

        # 2) Затем ищем число с явной валютой/суффиксом.
        suffix_pattern = re.compile(
            r"(?<![a-zа-я0-9])(\d+(?:[ \.,]\d{3})*)\s*(тыс|к|k|млн|m|₸|тг|тенге)\b",
            re.IGNORECASE,
        )
        suffix_prices = [self._parse_price_candidate(m.group(1), m.group(2)) for m in suffix_pattern.finditer(lowered)]
        suffix_prices = [value for value in suffix_prices if value is not None]
        if suffix_prices:
            return max(suffix_prices)

        # 3) Числа вида 130.000 / 40 000 / 280,000 без явного слова "цена".
        grouped_thousands_pattern = re.compile(
            r"(?<![a-zа-я0-9])(\d{1,3}(?:[ \.,]\d{3})+)(?![a-zа-я0-9])",
            re.IGNORECASE,
        )
        grouped_prices = [self._parse_price_candidate(m.group(1), None) for m in grouped_thousands_pattern.finditer(lowered)]
        grouped_prices = [value for value in grouped_prices if value is not None]
        if grouped_prices:
            return max(grouped_prices)

        # 4) Короткая цена (820) в контексте продажи/торга -> интерпретируем как 820 000.
        short_sale_context_pattern = re.compile(
            r"(?:продам|продаю|продается|отдам|лотом\s+по|без\s+торга|торг|срочно)\D{0,24}(\d{3,4})(?!\d)",
            re.IGNORECASE,
        )
        short_context_prices = [
            self._parse_price_candidate(m.group(1), None, assume_thousands=True)
            for m in short_sale_context_pattern.finditer(lowered)
        ]
        short_context_prices = [value for value in short_context_prices if value is not None]
        if short_context_prices:
            return max(short_context_prices)

        # 5) Фоллбек: только "похоже на цену" (5+ цифр), чтобы не ловить 10400F/1660 и т.д.
        fallback_pattern = re.compile(r"(?<![a-zа-я0-9])(\d{5,})(?![a-zа-я0-9])", re.IGNORECASE)
        fallback_prices = [self._parse_price_candidate(m.group(1), None) for m in fallback_pattern.finditer(lowered)]
        fallback_prices = [value for value in fallback_prices if value is not None]
        if fallback_prices:
            return max(fallback_prices)

        return None

    def _parse_price_candidate(
        self,
        numeric_part: str,
        suffix: str | None,
        assume_thousands: bool = False,
    ) -> int | None:
        cleaned = numeric_part.replace(" ", "").replace(",", "").replace(".", "")
        if not cleaned.isdigit():
            return None

        value = int(cleaned)
        normalized_suffix = (suffix or "").strip().lower()
        if normalized_suffix in {"тыс", "к", "k"}:
            value *= 1000
        elif normalized_suffix in {"млн", "m"}:
            value *= 1_000_000
        elif assume_thousands and 100 <= value < 10000:
            value *= 1000

        if value <= 0:
            return None
        return value

    def _split_keywords(self, raw_keywords: str) -> list[str]:
        return [item.strip().lower() for item in raw_keywords.split(",") if item.strip()]

    def _match_monitor_rule(
        self,
        rule: dict,
        source_id: int,
        text: str,
        price: int | None,
        has_photo: bool,
    ) -> tuple[bool, list[str]]:
        source_ids = [int(value) for value in rule.get("source_ids", [])]
        if source_ids and source_id not in source_ids:
            return False, []

        text_lc = text.lower()
        include_keywords = self._split_keywords(rule.get("include_keywords", ""))
        exclude_keywords = self._split_keywords(rule.get("exclude_keywords", ""))
        matched_keywords = [keyword for keyword in include_keywords if keyword in text_lc]

        if include_keywords and not matched_keywords:
            return False, []
        if any(keyword in text_lc for keyword in exclude_keywords):
            return False, []
        if int(rule.get("require_photo", 0)) and not has_photo:
            return False, []

        min_price = rule.get("min_price")
        max_price = rule.get("max_price")
        if price is not None:
            if min_price is not None and price < int(min_price):
                return False, []
            if max_price is not None and price > int(max_price):
                return False, []

        return True, matched_keywords

    async def _notify_monitor_match(
        self,
        source: dict,
        rule: dict,
        text: str,
        price: int | None,
        has_photo: bool,
        matched_keywords: list[str],
        external_chat_id: str,
        external_message_id: int,
    ) -> bool:
        if not self._client:
            return False

        destination = (rule.get("notify_to") or "me").strip() or "me"
        source_link = f"https://t.me/c/{str(external_chat_id).replace('-100', '')}/{external_message_id}"
        message_lines = [
            "Найдено новое объявление по правилу мониторинга.",
            f"Источник: {source['name']} ({source['chat_ref']})",
            f"Правило: {rule['name']}",
            f"Цена: {price if price is not None else 'не определена'}",
            f"Фото: {'да' if has_photo else 'нет'}",
            f"Ключи: {', '.join(matched_keywords) if matched_keywords else 'нет'}",
            f"Ссылка: {source_link}",
            "",
            text[:1500] if text else "(без текста)",
        ]
        try:
            await self._client.send_message(destination, "\n".join(message_lines))
            return True
        except Exception as exc:
            self.logger.exception("Не удалось отправить уведомление мониторинга: %s", exc)
            return False

    async def _run_monitor_test(self, payload: str) -> None:
        if not self._client:
            return
        try:
            parsed = json.loads(payload or "{}")
            run_id = int(parsed.get("run_id", 0))
        except Exception:
            self.logger.error("Некорректный payload run_monitor_test: %s", payload)
            return
        if run_id <= 0:
            return

        run_payload = self.repository.get_monitor_test_payload(run_id)
        if not run_payload:
            self.repository.fail_monitor_test_run(run_id, "Тестовый запуск не найден.")
            return

        rule = run_payload["rule_payload"]
        source_ids = [int(source_id) for source_id in run_payload["source_ids"]]
        scan_limit = int(run_payload["scan_limit"])
        sources = self.repository.list_monitor_sources_by_ids(source_ids)
        if not sources:
            self.repository.fail_monitor_test_run(run_id, "Не найдены источники для теста.")
            return

        self.repository.start_monitor_test_run(run_id)
        total_scanned = 0
        total_matches = 0
        try:
            for source in sources:
                source_entity = self._resolve_chat_ref_for_telethon(str(source["chat_ref"]))
                async for message in self._client.iter_messages(source_entity, limit=scan_limit):
                    text = (message.raw_text or "").strip()
                    has_photo = bool(message.photo or message.media)
                    if not text and not has_photo:
                        continue

                    total_scanned += 1
                    detected_price = self._extract_price(text)
                    matched, matched_keywords = self._match_monitor_rule(
                        rule=rule,
                        source_id=int(source["id"]),
                        text=text,
                        price=detected_price,
                        has_photo=has_photo,
                    )
                    if not matched:
                        continue

                    total_matches += 1
                    self.repository.add_monitor_test_result(
                        run_id=run_id,
                        source_id=int(source["id"]),
                        source_name=str(source["name"]),
                        external_chat_id=str(message.chat_id or ""),
                        external_message_id=int(message.id),
                        message_text=text[:4000],
                        detected_price=detected_price,
                        has_photo=has_photo,
                        matched_keywords=",".join(matched_keywords),
                    )

            self.repository.finish_monitor_test_run(run_id, total_scanned, total_matches)
            self.logger.info(
                "Тест мониторинга завершен: run_id=%s, checked=%s, matches=%s.",
                run_id,
                total_scanned,
                total_matches,
            )
        except Exception as exc:
            self.repository.fail_monitor_test_run(run_id, str(exc))
            self.logger.exception("Ошибка теста мониторинга run_id=%s: %s", run_id, exc)

    def _resolve_media_path(self, relative_path: str) -> Path:
        media_path = Path(relative_path)
        if not media_path.is_absolute():
            media_path = self.settings.base_dir / media_path
        return media_path

    def _resolve_chat_ref_for_telethon(self, chat_ref: str) -> int | str:
        value = chat_ref.strip()
        if value.startswith("-") and value[1:].isdigit():
            return int(value)
        return value

    def _is_session_locked_error(self, exc: Exception) -> bool:
        if isinstance(exc, sqlite3.OperationalError):
            return "database is locked" in str(exc).lower()

        current = exc
        while current:
            if isinstance(current, sqlite3.OperationalError) and "database is locked" in str(current).lower():
                return True
            current = current.__cause__ or current.__context__

        return False

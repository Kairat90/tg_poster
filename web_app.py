import shutil
import json
from concurrent.futures import Future
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from queue import Queue
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import get_settings
from logger_config import configure_logging
from repository import Repository
from telegram_service import TelegramService


class QueueOnlyTelegramService:
    def __init__(self, repository) -> None:
        self.repository = repository

    def start(self) -> None:
        return

    def stop(self) -> None:
        return

    def get_status(self) -> str:
        return "Worker отделен от web-процесса"

    def refresh_scheduler(self) -> None:
        self.repository.enqueue_system_command("reload_scheduler")
        self.repository.enqueue_system_command("reload_monitoring")

    def publish_now(self, ad_id: int) -> Future:
        slot_key = f"manual:{ad_id}:{datetime.now().isoformat(timespec='seconds')}"
        self.repository.enqueue_publish_job(ad_id, "manual", slot_key)
        future: Future = Future()
        future.set_result(None)
        return future

    def list_forum_topics(self, chat_ref: str) -> list[dict]:
        raise RuntimeError("Функция доступна только в процессе worker.")


def normalize_times(raw_times: str) -> list[str]:
    normalized: list[str] = []
    for raw_time in raw_times.split(","):
        raw_time = raw_time.strip()
        if not raw_time:
            continue

        parts = raw_time.split(":", maxsplit=1)
        if len(parts) != 2:
            raise ValueError(f"Неверный формат времени: {raw_time}")

        try:
            hour = int(parts[0])
            minute = int(parts[1])
        except ValueError as exc:
            raise ValueError(f"Неверный формат времени: {raw_time}") from exc

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Недопустимое значение времени: {raw_time}")

        normalized.append(f"{hour:02d}:{minute:02d}")

    if not normalized:
        raise ValueError("Добавьте хотя бы одно время публикации.")

    return sorted(set(normalized))


def build_query(**params: str | int | None) -> str:
    filtered = {key: value for key, value in params.items() if value not in (None, "", [])}
    if not filtered:
        return ""
    return "?" + urlencode(filtered)


async def save_uploads(upload_dir: Path, uploaded_files: list[UploadFile]) -> list[str]:
    upload_dir.mkdir(parents=True, exist_ok=True)
    stored_paths: list[str] = []

    for upload in uploaded_files:
        if not upload.filename:
            continue

        suffix = Path(upload.filename).suffix
        with NamedTemporaryFile(delete=False, dir=upload_dir, suffix=suffix) as tmp_file:
            shutil.copyfileobj(upload.file, tmp_file)
            stored_paths.append(tmp_file.name)
        await upload.close()

    return stored_paths


def cleanup_files(paths: list[str]) -> None:
    for path in paths:
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            continue


def base_context(request: Request) -> dict:
    return {
        "request": request,
        "status": request.app.state.telegram_service.get_status(),
        "message": request.query_params.get("message", ""),
        "error": request.query_params.get("error", ""),
    }


def serialize_status(request: Request) -> dict:
    return {"status": request.app.state.telegram_service.get_status()}


def serialize_target(row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "chat_ref": row["chat_ref"],
        "topic_id": row["topic_id"],
        "topic_title": row["topic_title"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def serialize_ad_summary(row) -> dict:
    return {
        "id": row["id"],
        "title": row["title"],
        "interval_days": row["interval_days"],
        "is_active": bool(row["is_active"]),
        "target_count": row["target_count"],
        "schedule_count": row["schedule_count"],
    }


def serialize_ad_details(ad: dict | None) -> dict | None:
    if not ad:
        return None

    return {
        "id": ad["id"],
        "title": ad["title"],
        "text": ad["text"],
        "interval_days": ad["interval_days"],
        "is_active": bool(ad["is_active"]),
        "media_paths": list(ad["media_paths"]),
        "target_ids": list(ad["target_ids"]),
        "targets": list(ad["targets"]),
        "times": list(ad["times"]),
    }


def serialize_log(row) -> dict:
    return {
        "id": row["id"],
        "published_at": row["published_at"],
        "status": row["status"],
        "message": row["message"],
        "ad_title": row["ad_title"],
        "target_name": row["target_name"],
        "target_chat_ref": row["target_chat_ref"],
    }


def serialize_monitor_source(row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "chat_ref": row["chat_ref"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def serialize_monitor_rule(row, source_ids: list[int]) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "include_keywords": row["include_keywords"],
        "exclude_keywords": row["exclude_keywords"],
        "min_price": row["min_price"],
        "max_price": row["max_price"],
        "require_photo": bool(row["require_photo"]),
        "notify_to": row["notify_to"],
        "source_ids": source_ids,
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def serialize_monitor_match(row) -> dict:
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "message_text": row["message_text"],
        "detected_price": row["detected_price"],
        "has_photo": bool(row["has_photo"]),
        "matched_keywords": row["matched_keywords"],
        "notified": bool(row["notified"]),
        "external_chat_id": row["external_chat_id"],
        "external_message_id": row["external_message_id"],
        "source_name": row["source_name"],
        "source_chat_ref": row["source_chat_ref"],
        "rule_name": row["rule_name"],
    }


def serialize_monitor_test_run(row) -> dict:
    if not row:
        return {}
    return {
        "id": row["id"],
        "status": row["status"],
        "scan_limit": row["scan_limit"],
        "total_scanned": row["total_scanned"],
        "total_matches": row["total_matches"],
        "error_message": row["error_message"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def serialize_monitor_test_result(row) -> dict:
    external_chat_id = str(row["external_chat_id"] or "")
    external_message_id = int(row["external_message_id"])
    message_link = ""
    if external_chat_id.startswith("-100") and len(external_chat_id) > 4:
        message_link = f"https://t.me/c/{external_chat_id[4:]}/{external_message_id}"
    return {
        "id": row["id"],
        "run_id": row["run_id"],
        "source_id": row["source_id"],
        "source_name": row["source_name"],
        "external_chat_id": external_chat_id,
        "external_message_id": external_message_id,
        "message_link": message_link,
        "message_text": row["message_text"],
        "detected_price": row["detected_price"],
        "has_photo": bool(row["has_photo"]),
        "matched_keywords": row["matched_keywords"],
        "created_at": row["created_at"],
    }


def api_error(message: str, status_code: int = 400) -> HTTPException:
    return HTTPException(status_code=status_code, detail=message)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    settings.media_dir.mkdir(exist_ok=True)
    settings.static_dir.mkdir(exist_ok=True)
    settings.templates_dir.mkdir(exist_ok=True)
    settings.upload_tmp_dir.mkdir(exist_ok=True)

    repository = Repository(settings.db_path, settings.media_dir)
    repository.init_db()

    log_queue: Queue[str] = Queue()
    logger = configure_logging(settings.log_path, log_queue)

    if settings.embedded_worker_enabled:
        telegram_service = TelegramService(settings, repository, logger)
        telegram_service.start()
        logger.info("Worker запущен в embedded-режиме.")
    else:
        telegram_service = QueueOnlyTelegramService(repository)
        logger.info("Worker отключен в web-процессе. Используется очередь публикаций.")

    app.state.settings = settings
    app.state.repository = repository
    app.state.logger = logger
    app.state.log_queue = log_queue
    app.state.telegram_service = telegram_service

    try:
        yield
    finally:
        try:
            telegram_service.stop()
        except Exception:
            logger.exception("Ошибка остановки Telegram-сервиса.")


settings = get_settings()
settings.static_dir.mkdir(exist_ok=True)
settings.templates_dir.mkdir(exist_ok=True)
settings.upload_tmp_dir.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(settings.templates_dir))
app = FastAPI(title="TG Poster", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")
if (settings.frontend_dist_dir / "assets").exists():
    app.mount("/app/assets", StaticFiles(directory=str(settings.frontend_dist_dir / "assets")), name="frontend-assets")


@app.get("/")
async def index() -> RedirectResponse:
    return RedirectResponse(url="/app", status_code=303)


@app.get("/app", response_class=HTMLResponse)
async def vue_app(request: Request):
    index_file = request.app.state.settings.frontend_dist_dir / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=503, detail="Vue frontend is not built yet.")

    html = index_file.read_text(encoding="utf-8")
    return html.replace('src="/assets/', 'src="/app/assets/').replace('href="/assets/', 'href="/app/assets/')


@app.get("/api/status")
async def api_status(request: Request) -> dict:
    return serialize_status(request)


@app.get("/api/bootstrap")
async def api_bootstrap(request: Request) -> dict:
    repository = request.app.state.repository
    monitor_rule_rows = repository.list_monitor_rules()
    return {
        "status": serialize_status(request)["status"],
        "ads": [serialize_ad_summary(row) for row in repository.list_ads()],
        "targets": [serialize_target(row) for row in repository.list_targets()],
        "logs": [serialize_log(row) for row in reversed(repository.list_publish_logs())],
        "monitor_sources": [serialize_monitor_source(row) for row in repository.list_monitor_sources()],
        "monitor_rules": [
            serialize_monitor_rule(row, repository.get_monitor_rule_source_ids(int(row["id"])))
            for row in monitor_rule_rows
        ],
        "monitor_matches": [serialize_monitor_match(row) for row in reversed(repository.list_monitor_matches())],
    }


@app.get("/api/ads")
async def api_list_ads(request: Request) -> dict:
    repository = request.app.state.repository
    return {"items": [serialize_ad_summary(row) for row in repository.list_ads()]}


@app.get("/api/ads/{ad_id}")
async def api_get_ad(request: Request, ad_id: int) -> dict:
    repository = request.app.state.repository
    ad = repository.get_ad(ad_id)
    if not ad:
        raise api_error("Объявление не найдено.", 404)
    return {"item": serialize_ad_details(ad)}


@app.post("/api/ads")
async def api_save_ad(
    request: Request,
    title: str = Form(...),
    text: str = Form(...),
    times: str = Form(...),
    interval_days: str = Form(...),
    is_active: str = Form(default="true"),
    target_ids: list[int] = Form(default=[]),
    existing_media: str = Form(default=""),
    ad_id: int | None = Form(default=None),
    media_files: list[UploadFile] = File(default=[]),
):
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    upload_tmp_dir = request.app.state.settings.upload_tmp_dir

    title = title.strip()
    text = text.strip()
    existing_media_paths = [line.strip() for line in existing_media.splitlines() if line.strip()]

    if not title:
        raise api_error("Введите заголовок.")
    if not text:
        raise api_error("Введите текст.")
    if not target_ids:
        raise api_error("Выберите хотя бы один чат.")

    try:
        interval_value = int(interval_days)
        if interval_value < 1:
            raise ValueError
    except ValueError as exc:
        raise api_error("Интервал в днях должен быть положительным целым числом.") from exc

    try:
        normalized_times = normalize_times(times)
    except ValueError as exc:
        raise api_error(str(exc)) from exc

    uploaded_paths = await save_uploads(upload_tmp_dir, media_files)
    media_sources = existing_media_paths + uploaded_paths

    try:
        saved_ad_id = repository.save_ad(
            ad_id,
            title,
            text,
            interval_value,
            is_active.lower() in {"true", "1", "on", "yes"},
            media_sources,
            target_ids,
            normalized_times,
        )
        saved_ad = repository.get_ad(saved_ad_id)
    except Exception as exc:
        cleanup_files(uploaded_paths)
        raise api_error(f"Ошибка сохранения: {exc}") from exc

    cleanup_files(uploaded_paths)
    telegram_service.refresh_scheduler()
    return {
        "message": "Объявление сохранено, расписание обновлено.",
        "item": serialize_ad_details(saved_ad),
    }


@app.delete("/api/ads/{ad_id}")
async def api_delete_ad(request: Request, ad_id: int) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    repository.delete_ad(ad_id)
    telegram_service.refresh_scheduler()
    return {"message": "Объявление удалено."}


@app.post("/api/ads/{ad_id}/publish")
async def api_publish_ad(request: Request, ad_id: int) -> dict:
    telegram_service = request.app.state.telegram_service
    try:
        telegram_service.publish_now(ad_id)
    except Exception as exc:
        raise api_error(f"Ошибка публикации: {exc}") from exc
    return {"message": "Ручная публикация поставлена в очередь."}


@app.get("/api/targets")
async def api_list_targets(request: Request) -> dict:
    repository = request.app.state.repository
    return {"items": [serialize_target(row) for row in repository.list_targets()]}


@app.get("/api/targets/{target_id}")
async def api_get_target(request: Request, target_id: int) -> dict:
    repository = request.app.state.repository
    target = repository.get_target(target_id)
    if not target:
        raise api_error("Канал не найден.", 404)
    return {"item": serialize_target(target)}


@app.post("/api/targets")
async def api_save_target(
    request: Request,
    name: str = Form(...),
    chat_ref: str = Form(...),
    is_active: str = Form(default="true"),
    topic_id: str = Form(default=""),
    topic_title: str = Form(default=""),
    target_id: int | None = Form(default=None),
) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service

    name = name.strip()
    chat_ref = chat_ref.strip()
    topic_title = topic_title.strip()
    if not name or not chat_ref:
        raise api_error("Введите название и чат/ссылку.")

    parsed_topic_id: int | None = None
    if topic_id.strip():
        try:
            parsed_topic_id = int(topic_id.strip())
        except ValueError as exc:
            raise api_error("ID темы должен быть целым числом.") from exc

    try:
        saved_target_id = repository.save_target(
            target_id,
            name,
            chat_ref,
            parsed_topic_id,
            topic_title,
            is_active.lower() in {"true", "1", "on", "yes"},
        )
        saved_target = repository.get_target(saved_target_id)
    except Exception as exc:
        raise api_error(f"Ошибка сохранения: {exc}") from exc

    telegram_service.refresh_scheduler()
    return {"message": "Канал сохранен.", "item": serialize_target(saved_target)}


@app.delete("/api/targets/{target_id}")
async def api_delete_target(request: Request, target_id: int) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    repository.delete_target(target_id)
    telegram_service.refresh_scheduler()
    return {"message": "Канал удален."}


@app.get("/api/target-topics")
async def api_target_topics(request: Request, chat_ref: str) -> dict:
    telegram_service = request.app.state.telegram_service
    chat_ref = chat_ref.strip()
    if not chat_ref:
        raise api_error("Укажите chat_ref.")
    try:
        topics = telegram_service.list_forum_topics(chat_ref)
    except Exception as exc:
        raise api_error(f"Не удалось загрузить темы: {exc}") from exc
    return {"items": topics}


@app.get("/api/logs")
async def api_logs(request: Request) -> dict:
    repository = request.app.state.repository
    return {"items": [serialize_log(row) for row in reversed(repository.list_publish_logs())]}


@app.post("/api/scheduler/reload")
async def api_reload_scheduler(request: Request) -> dict:
    request.app.state.telegram_service.refresh_scheduler()
    return {"message": "Запрошено обновление расписания."}


@app.get("/api/monitor/sources")
async def api_list_monitor_sources(request: Request) -> dict:
    repository = request.app.state.repository
    return {"items": [serialize_monitor_source(row) for row in repository.list_monitor_sources()]}


@app.post("/api/monitor/sources")
async def api_save_monitor_source(
    request: Request,
    name: str = Form(...),
    chat_ref: str = Form(...),
    is_active: str = Form(default="true"),
    source_id: int | None = Form(default=None),
) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    name = name.strip()
    chat_ref = chat_ref.strip()
    if not name or not chat_ref:
        raise api_error("Укажите название источника и chat_ref.")
    saved_id = repository.save_monitor_source(source_id, name, chat_ref, is_active.lower() in {"true", "1", "on", "yes"})
    telegram_service.refresh_scheduler()
    saved = next((row for row in repository.list_monitor_sources() if row["id"] == saved_id), None)
    if not saved:
        raise api_error("Не удалось получить сохраненный источник.", 500)
    return {"message": "Источник мониторинга сохранен.", "item": serialize_monitor_source(saved)}


@app.delete("/api/monitor/sources/{source_id}")
async def api_delete_monitor_source(request: Request, source_id: int) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    repository.delete_monitor_source(source_id)
    telegram_service.refresh_scheduler()
    return {"message": "Источник мониторинга удален."}


@app.get("/api/monitor/rules")
async def api_list_monitor_rules(request: Request) -> dict:
    repository = request.app.state.repository
    rows = repository.list_monitor_rules()
    items = [
        serialize_monitor_rule(row, repository.get_monitor_rule_source_ids(int(row["id"])))
        for row in rows
    ]
    return {"items": items}


@app.post("/api/monitor/rules")
async def api_save_monitor_rule(
    request: Request,
    name: str = Form(...),
    include_keywords: str = Form(default=""),
    exclude_keywords: str = Form(default=""),
    min_price: str = Form(default=""),
    max_price: str = Form(default=""),
    require_photo: str = Form(default="false"),
    notify_to: str = Form(default="me"),
    is_active: str = Form(default="true"),
    source_ids: list[int] = Form(default=[]),
    rule_id: int | None = Form(default=None),
) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    name = name.strip()
    if not name:
        raise api_error("Укажите название правила.")

    min_price_value = int(min_price) if min_price.strip() else None
    max_price_value = int(max_price) if max_price.strip() else None
    if min_price_value is not None and max_price_value is not None and min_price_value > max_price_value:
        raise api_error("Минимальная цена не может быть больше максимальной.")
    if not source_ids:
        raise api_error("Выберите хотя бы один источник для правила.")

    saved_id = repository.save_monitor_rule(
        rule_id=rule_id,
        name=name,
        include_keywords=include_keywords.strip(),
        exclude_keywords=exclude_keywords.strip(),
        min_price=min_price_value,
        max_price=max_price_value,
        require_photo=require_photo.lower() in {"true", "1", "on", "yes"},
        notify_to=notify_to.strip() or "me",
        is_active=is_active.lower() in {"true", "1", "on", "yes"},
        source_ids=source_ids,
    )
    telegram_service.refresh_scheduler()
    saved = next((row for row in repository.list_monitor_rules() if row["id"] == saved_id), None)
    if not saved:
        raise api_error("Не удалось получить сохраненное правило.", 500)
    saved_source_ids = repository.get_monitor_rule_source_ids(int(saved["id"]))
    return {"message": "Правило мониторинга сохранено.", "item": serialize_monitor_rule(saved, saved_source_ids)}


@app.delete("/api/monitor/rules/{rule_id}")
async def api_delete_monitor_rule(request: Request, rule_id: int) -> dict:
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    repository.delete_monitor_rule(rule_id)
    telegram_service.refresh_scheduler()
    return {"message": "Правило мониторинга удалено."}


@app.get("/api/monitor/matches")
async def api_list_monitor_matches(request: Request) -> dict:
    repository = request.app.state.repository
    return {"items": [serialize_monitor_match(row) for row in reversed(repository.list_monitor_matches())]}


@app.post("/api/monitor/test-rule")
async def api_monitor_test_rule(
    request: Request,
    name: str = Form(...),
    include_keywords: str = Form(default=""),
    exclude_keywords: str = Form(default=""),
    min_price: str = Form(default=""),
    max_price: str = Form(default=""),
    require_photo: str = Form(default="false"),
    source_ids: list[int] = Form(default=[]),
    scan_limit: str = Form(default="50"),
) -> dict:
    repository = request.app.state.repository

    name = name.strip()
    if not name:
        raise api_error("Укажите название правила для теста.")
    if not source_ids:
        raise api_error("Выберите хотя бы один источник для теста.")

    try:
        scan_limit_value = int(scan_limit)
    except ValueError as exc:
        raise api_error("Лимит должен быть целым числом.") from exc
    if scan_limit_value < 1 or scan_limit_value > 100:
        raise api_error("Лимит сообщений должен быть в диапазоне 1..100.")

    min_price_value = int(min_price) if min_price.strip() else None
    max_price_value = int(max_price) if max_price.strip() else None
    if min_price_value is not None and max_price_value is not None and min_price_value > max_price_value:
        raise api_error("Минимальная цена не может быть больше максимальной.")

    rule_payload = {
        "name": name,
        "include_keywords": include_keywords.strip(),
        "exclude_keywords": exclude_keywords.strip(),
        "min_price": min_price_value,
        "max_price": max_price_value,
        "require_photo": require_photo.lower() in {"true", "1", "on", "yes"},
        "source_ids": source_ids,
    }
    run_id = repository.create_monitor_test_run(rule_payload, source_ids, scan_limit_value)
    repository.enqueue_system_command(
        "run_monitor_test",
        json.dumps({"run_id": run_id}, ensure_ascii=False),
    )
    return {"message": "Тест правила запущен.", "run_id": run_id}


@app.get("/api/monitor/test-rule/{run_id}")
async def api_monitor_test_rule_status(request: Request, run_id: int) -> dict:
    repository = request.app.state.repository
    run = repository.get_monitor_test_run(run_id)
    if not run:
        raise api_error("Тестовый запуск не найден.", 404)
    results = repository.list_monitor_test_results(run_id)
    return {
        "run": serialize_monitor_test_run(run),
        "items": [serialize_monitor_test_result(row) for row in reversed(results)],
    }


@app.get("/ads")
async def ads_page(request: Request, ad_id: int | None = None):
    repository = request.app.state.repository
    selected_ad = repository.get_ad(ad_id) if ad_id else None
    ads = repository.list_ads()
    targets = repository.list_targets()
    context = base_context(request)
    context.update(
        {
            "ads": ads,
            "targets": targets,
            "selected_ad": selected_ad,
            "selected_ad_id": ad_id,
        }
    )
    return templates.TemplateResponse(request, "ads.html", context)


@app.post("/ads/save")
async def save_ad(
    request: Request,
    title: str = Form(...),
    text: str = Form(...),
    times: str = Form(...),
    interval_days: str = Form(...),
    is_active: str | None = Form(default=None),
    target_ids: list[int] = Form(default=[]),
    existing_media: str = Form(default=""),
    ad_id: int | None = Form(default=None),
    media_files: list[UploadFile] = File(default=[]),
):
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    upload_tmp_dir = request.app.state.settings.upload_tmp_dir

    title = title.strip()
    text = text.strip()
    existing_media_paths = [line.strip() for line in existing_media.splitlines() if line.strip()]

    if not title:
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error="Введите заголовок."),
            status_code=303,
        )
    if not text:
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error="Введите текст."),
            status_code=303,
        )
    if not target_ids:
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error="Выберите хотя бы один чат."),
            status_code=303,
        )

    try:
        interval_value = int(interval_days)
        if interval_value < 1:
            raise ValueError
    except ValueError:
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error="Интервал в днях должен быть положительным целым числом."),
            status_code=303,
        )

    try:
        normalized_times = normalize_times(times)
    except ValueError as exc:
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error=str(exc)),
            status_code=303,
        )

    uploaded_paths = await save_uploads(upload_tmp_dir, media_files)
    media_sources = existing_media_paths + uploaded_paths

    try:
        saved_ad_id = repository.save_ad(
            ad_id,
            title,
            text,
            interval_value,
            is_active == "on",
            media_sources,
            target_ids,
            normalized_times,
        )
    except Exception as exc:
        cleanup_files(uploaded_paths)
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error=f"Ошибка сохранения: {exc}"),
            status_code=303,
        )

    cleanup_files(uploaded_paths)
    telegram_service.refresh_scheduler()
    return RedirectResponse(
        url="/ads" + build_query(ad_id=saved_ad_id, message="Объявление сохранено, расписание обновлено."),
        status_code=303,
    )


@app.post("/ads/{ad_id}/delete")
async def delete_ad(request: Request, ad_id: int):
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    repository.delete_ad(ad_id)
    telegram_service.refresh_scheduler()
    return RedirectResponse(url="/ads" + build_query(message="Объявление удалено."), status_code=303)


@app.post("/ads/{ad_id}/publish")
async def publish_ad(request: Request, ad_id: int):
    telegram_service = request.app.state.telegram_service
    try:
        telegram_service.publish_now(ad_id)
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, message="Ручная публикация поставлена в очередь."),
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url="/ads" + build_query(ad_id=ad_id, error=f"Ошибка публикации: {exc}"),
            status_code=303,
        )


@app.post("/scheduler/reload")
async def reload_scheduler(request: Request, ad_id: int | None = Form(default=None)):
    request.app.state.telegram_service.refresh_scheduler()
    return RedirectResponse(
        url="/ads" + build_query(ad_id=ad_id, message="Запрошено обновление расписания."),
        status_code=303,
    )


@app.get("/targets")
async def targets_page(request: Request, target_id: int | None = None):
    repository = request.app.state.repository
    selected_target = repository.get_target(target_id) if target_id else None
    context = base_context(request)
    context.update(
        {
            "targets": repository.list_targets(),
            "selected_target": selected_target,
            "selected_target_id": target_id,
        }
    )
    return templates.TemplateResponse(request, "targets.html", context)


@app.post("/targets/save")
async def save_target(
    request: Request,
    name: str = Form(...),
    chat_ref: str = Form(...),
    is_active: str | None = Form(default=None),
    topic_id: str = Form(default=""),
    topic_title: str = Form(default=""),
    target_id: int | None = Form(default=None),
):
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service

    name = name.strip()
    chat_ref = chat_ref.strip()
    topic_title = topic_title.strip()
    if not name or not chat_ref:
        return RedirectResponse(
            url="/targets" + build_query(target_id=target_id, error="Введите название и чат/ссылку."),
            status_code=303,
        )

    parsed_topic_id: int | None = None
    if topic_id.strip():
        try:
            parsed_topic_id = int(topic_id.strip())
        except ValueError:
            return RedirectResponse(
                url="/targets" + build_query(target_id=target_id, error="ID темы должен быть целым числом."),
                status_code=303,
            )

    try:
        saved_target_id = repository.save_target(
            target_id,
            name,
            chat_ref,
            parsed_topic_id,
            topic_title,
            is_active == "on",
        )
    except Exception as exc:
        return RedirectResponse(
            url="/targets" + build_query(target_id=target_id, error=f"Ошибка сохранения: {exc}"),
            status_code=303,
        )

    telegram_service.refresh_scheduler()
    return RedirectResponse(
        url="/targets" + build_query(target_id=saved_target_id, message="Канал сохранен."),
        status_code=303,
    )


@app.post("/targets/{target_id}/delete")
async def delete_target(request: Request, target_id: int):
    repository = request.app.state.repository
    telegram_service = request.app.state.telegram_service
    repository.delete_target(target_id)
    telegram_service.refresh_scheduler()
    return RedirectResponse(url="/targets" + build_query(message="Канал удален."), status_code=303)


@app.get("/logs")
async def logs_page(request: Request):
    repository = request.app.state.repository
    context = base_context(request)
    context.update({"logs": list(reversed(repository.list_publish_logs()))})
    return templates.TemplateResponse(request, "logs.html", context)

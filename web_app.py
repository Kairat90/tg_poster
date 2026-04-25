import shutil
from contextlib import asynccontextmanager
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

    telegram_service = TelegramService(settings, repository, logger)
    telegram_service.start()

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
    return {
        "status": serialize_status(request)["status"],
        "ads": [serialize_ad_summary(row) for row in repository.list_ads()],
        "targets": [serialize_target(row) for row in repository.list_targets()],
        "logs": [serialize_log(row) for row in reversed(repository.list_publish_logs())],
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

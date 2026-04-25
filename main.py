from queue import Queue

from config import get_settings
from logger_config import configure_logging
from repository import Repository
from telegram_service import TelegramService
from ui import AutoPosterApp


def main() -> None:
    settings = get_settings()
    repository = Repository(settings.db_path, settings.media_dir)
    repository.init_db()

    log_queue: Queue[str] = Queue()
    logger = configure_logging(settings.log_path, log_queue)

    telegram_service = TelegramService(settings, repository, logger)
    telegram_service.start()

    app = AutoPosterApp(repository, telegram_service, settings, log_queue)
    app.mainloop()


if __name__ == "__main__":
    main()

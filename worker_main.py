import time
from queue import Queue

from config import get_settings
from logger_config import configure_logging
from repository import Repository
from telegram_service import TelegramService


def main() -> None:
    settings = get_settings()
    settings.media_dir.mkdir(exist_ok=True)
    settings.upload_tmp_dir.mkdir(exist_ok=True)

    repository = Repository(settings.db_path, settings.media_dir)
    repository.init_db()

    logger = configure_logging(settings.log_path, Queue())
    telegram_service = TelegramService(settings, repository, logger)
    telegram_service.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        telegram_service.stop()


if __name__ == "__main__":
    main()

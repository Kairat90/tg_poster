import logging
from queue import Queue


class QueueLogHandler(logging.Handler):
    def __init__(self, queue: Queue[str]) -> None:
        super().__init__()
        self.queue = queue

    def emit(self, record: logging.LogRecord) -> None:
        self.queue.put(self.format(record))


def configure_logging(log_path, queue: Queue[str]) -> logging.Logger:
    logger = logging.getLogger("tg_poster")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    queue_handler = QueueLogHandler(queue)
    queue_handler.setFormatter(formatter)
    logger.addHandler(queue_handler)

    return logger

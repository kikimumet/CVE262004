import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


class DailyRotatingFileHandler(TimedRotatingFileHandler):

    def __init__(self, log_dir: str, level=logging.DEBUG):
        self.log_dir = log_dir
        log_filepath = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
        super().__init__(
            filename=log_filepath,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8"
        )

    def doRollover(self):
        self.stream.close()
        self.baseFilename = os.path.join(
            self.log_dir,
            f"{datetime.now().strftime('%Y-%m-%d')}.log"
        )
        self.stream = self._open()
        self.rolloverAt = self.computeRollover(self.rolloverAt)


def setupLogger(level=logging.DEBUG, log_dir: str = "apps/logs"):
    os.makedirs(log_dir, exist_ok=True)

    rotating_handler = DailyRotatingFileHandler(log_dir=log_dir, level=level)
    stream_handler   = logging.StreamHandler()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    rotating_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()

    if not root_logger.handlers:
        root_logger.setLevel(level)
        root_logger.addHandler(rotating_handler)
        root_logger.addHandler(stream_handler)
import logging
import os
from datetime import datetime

def setupLogger(level=logging.DEBUG, log_dir: str = "apps/logs"):
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    log_filepath = os.path.join(log_dir, log_filename)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filepath, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
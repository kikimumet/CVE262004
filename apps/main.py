import asyncio
import logging
import threading

from apps.logger import setupLogger
from apps.services.polymer.case1 import start_stream as start_case1
from apps.services.polymer.case2 import start_stream as start_case2

setupLogger(level=logging.INFO, log_dir="apps/logs")
logger = logging.getLogger(__name__)


def run_case1():
    logger.info("[main] Memulai case1po (Polymer Case 1)...")
    try:
        start_case1()
    except Exception as e:
        logger.error(f"[main] case1po berhenti karena error: {e}")


def run_case2():
    logger.info("[main] Memulai case2po (Polymer Case 2)...")
    try:
        start_case2()
    except Exception as e:
        logger.error(f"[main] case2po berhenti karena error: {e}")


if __name__ == "__main__":
    logger.info("[main] Menjalankan semua stream secara paralel...")

    t1 = threading.Thread(target=run_case1, name="Thread-case1po", daemon=True)
    t2 = threading.Thread(target=run_case2, name="Thread-case2po", daemon=True)

    t1.start()
    t2.start()

    logger.info("[main] Semua stream berjalan. Tekan Ctrl+C untuk berhenti.")

    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        logger.info("[main] Dihentikan manual. Semua stream akan berhenti.")
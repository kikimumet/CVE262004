import logging
import threading

from apps.logger import setupLogger
from apps.services.ahmmoprd001.ahmmoprd001 import start_stream as start_ahmmoprd001
from apps.services.ahmmoprd002.ahmmoprd002 import start_stream as start_ahmmoprd002

setupLogger(level=logging.INFO, log_dir="apps/logs")
logger = logging.getLogger(__name__)


def run_ahmmoprd001():
    logger.info("[main] Memulai ahmmoprd001 (Case 1)...")
    try:
        start_ahmmoprd001()
    except Exception as e:
        logger.error(f"[main] ahmmoprd001 berhenti karena error: {e}")


def run_ahmmoprd002():
    logger.info("[main] Memulai ahmmoprd002 (Case 2)...")
    try:
        start_ahmmoprd002()
    except Exception as e:
        logger.error(f"[main] ahmmoprd002 berhenti karena error: {e}")


if __name__ == "__main__":
    logger.info("[main] Menjalankan semua stream secara paralel...")

    t1 = threading.Thread(target=run_ahmmoprd001, name="Thread-ahmmoprd001", daemon=True)
    t2 = threading.Thread(target=run_ahmmoprd002, name="Thread-ahmmoprd002", daemon=True)

    t1.start()
    t2.start()

    logger.info("[main] Semua stream berjalan. Tekan Ctrl+C untuk berhenti.")

    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        logger.info("[main] Dihentikan manual. Semua stream akan berhenti.")
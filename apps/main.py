import logging
import threading

from apps.logger import setupLogger
from apps.services.ahmmoprd001.ahmmoprd001 import start_stream as start_ahmmoprd001
from apps.services.ahmmoprd002.ahmmoprd002 import start_stream as start_ahmmoprd002
from apps.services.ahmmoprd003.ahmmoprd003 import start_stream as start_ahmmoprd003

setupLogger(level=logging.INFO, log_dir="apps/logs")
logger = logging.getLogger(__name__)


def run_ahmmoprd001():
    logger.info("[main] Memulai ahmmoprd001...")
    try:
        start_ahmmoprd001()
    except Exception as e:
        logger.error(f"[main] ahmmoprd001 berhenti karena error: {e}")


def run_ahmmoprd002():
    logger.info("[main] Memulai ahmmoprd002...")
    try:
        start_ahmmoprd002()
    except Exception as e:
        logger.error(f"[main] ahmmoprd002 berhenti karena error: {e}")


def run_ahmmoprd003():
    logger.info("[main] Memulai ahmmoprd003...")
    try:
        start_ahmmoprd003()
    except Exception as e:
        logger.error(f"[main] ahmmoprd003 berhenti karena error: {e}")


if __name__ == "__main__":
    logger.info("[main] Menjalankan semua stream secara paralel...")

    t1 = threading.Thread(target=run_ahmmoprd001, name="Thread-ahmmoprd001", daemon=True)
    t2 = threading.Thread(target=run_ahmmoprd002, name="Thread-ahmmoprd002", daemon=True)
    t3 = threading.Thread(target=run_ahmmoprd003, name="Thread-ahmmoprd003", daemon=True)

    t1.start()
    t2.start()
    t3.start()

    logger.info("[main] Semua stream berjalan. Tekan Ctrl+C untuk berhenti.")

    try:
        t1.join()
        t2.join()
        t3.join()
    except KeyboardInterrupt:
        logger.info("[main] Dihentikan manual. Semua stream akan berhenti.")
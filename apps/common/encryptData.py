import logging
from cryptography.fernet import Fernet
from typing import Union

logger = logging.getLogger(__name__)


def generateFernetKey() -> Union[str, bool]:
    try:
        key = Fernet.generate_key().decode()
        logger.info("Fernet key berhasil di-generate.")
        return key
    except Exception as e:
        logger.error(f"Gagal generate Fernet key: {e}")
        return False


def encryptData(key: str, plainText: str) -> Union[str, bool]:
    if not key:
        logger.error("Key tidak boleh kosong.")
        return False

    if plainText is None:
        logger.error("PlainText tidak boleh None.")
        return False

    try:
        cipher_suite = Fernet(key.encode())
        encrypted = cipher_suite.encrypt(plainText.encode())
        result = encrypted.decode()
        logger.info("Berhasil mengenkripsi data.")
        return result
    except Exception as e:
        logger.error(f"Gagal mengenkripsi data. Periksa Key Anda. Error: {e}")
        return False
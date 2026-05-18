import logging
from typing import Union
from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)


def decryptData(key: str, encryptText: str) -> Union[str, bool]:
    if not key:
        logger.error("Key tidak boleh kosong.")
        return False

    if not encryptText:
        logger.error("EncryptText tidak boleh kosong.")
        return False

    try:
        cipher_suite = Fernet(key.encode())
        decrypted = cipher_suite.decrypt(encryptText.encode())
        result = decrypted.decode()
        logger.info("Berhasil mendekripsi data.")
        return result
    except InvalidToken:
        logger.error("Gagal mendekripsi: Key salah atau data tidak valid / sudah rusak.")
        return False
    except Exception as e:
        logger.error(f"Gagal mendekripsi data. Periksa Key Anda. Error: {e}")
        return False
import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

def loadProperties(pathConfig: Union[str, Path]) -> Union[dict, bool]:
    if not pathConfig:
        logger.error("PathConfig tidak boleh kosong.")
        return False

    config_obj = Path(pathConfig)
    if not config_obj.exists():
        logger.error(f"File properties tidak ditemukan: {config_obj}")
        return False

    if not config_obj.is_file():
        logger.error(f"PathConfig bukan sebuah file: {config_obj}")
        return False

    try:
        logger.info(f"Membaca file properties: {config_obj.name}")
        result = {}

        with open(config_obj, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                stripped = line.strip()

                if not stripped:
                    continue

                if stripped.startswith("#"):
                    logger.debug(f"Baris {line_num} di-skip: {stripped}")
                    continue

                # Parsing key=value
                if "=" not in stripped:
                    logger.warning(f"Baris {line_num} tidak valid (tidak ada '='): {stripped}")
                    continue

                key, _, value = stripped.partition("=")
                key = key.strip()
                value = value.strip()

                if not key:
                    logger.warning(f"Baris {line_num} di-skip (key kosong).")
                    continue

                result[key] = value
                logger.debug(f"Loaded: {key} = {value}")

        logger.info(f"Berhasil membaca {len(result)} properties dari {config_obj.name}")
        return result

    except Exception as e:
        logger.error(f"Gagal membaca file properties: {e}")
        return False
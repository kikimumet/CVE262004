import pandas as pd
from pathlib import Path
import logging
from typing import Union

logger = logging.getLogger(__name__)

def loadFileJsonToString(pathSource: Union[str, Path]) -> Union[str, bool]:
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    path_obj = Path(pathSource)
    if not path_obj.exists():
        logger.error(f"File JSON tidak ditemukan: {path_obj}")
        return False

    try:
        logger.info(f"Membaca file JSON ke String: {path_obj.name}")
        df = pd.read_json(path_obj)
        return df.to_json(orient="records")
    except Exception as e:
        logger.error(f"Gagal membaca file JSON ke String: {e}")
        return False
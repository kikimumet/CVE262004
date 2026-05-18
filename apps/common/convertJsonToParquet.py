import pandas as pd
from pathlib import Path
import logging
from typing import Union

logger = logging.getLogger(__name__)

def convertJsonToParquet(pathSource: Union[str, Path], pathTarget: Union[str, Path]) -> bool:

    if not pathSource or not pathTarget:
        logger.error("PathSource atau PathTarget tidak boleh kosong.")
        return False

    source_obj = Path(pathSource)
    target_obj = Path(pathTarget)

    if not source_obj.exists():
        logger.error(f"File sumber tidak ditemukan: {source_obj}")
        return False

    target_obj.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f"Membaca file JSON dari {source_obj}...")
        df = pd.read_json(source_obj)
        
        logger.info("Mengubah format file menjadi Parquet...")
        df.to_parquet(target_obj, index=False)
        
        logger.info(f"SUKSES! File Parquet berhasil dibuat di: {target_obj}")
        return True
    except Exception as e:
        logger.error(f"Gagal mengubah file JSON ke Parquet: {e}")
        return False
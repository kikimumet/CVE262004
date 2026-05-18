import pandas as pd
from pathlib import Path
import logging
import io
from typing import Union

logger = logging.getLogger(__name__)

def writeStringJsonToFileParquet(stringJson: str, pathTarget: Union[str, Path]) -> bool:
    if not stringJson or not pathTarget:
        logger.error("String JSON atau PathTarget tidak boleh kosong.")
        return False

    target_obj = Path(pathTarget)
    target_obj.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Menulis String JSON ke format Parquet...")
        df = pd.read_json(io.StringIO(stringJson.strip()))
        df.to_parquet(target_obj, index=False)
        logger.info(f"SUKSES! File Parquet berhasil dibuat di: {target_obj}")
        return True
    except Exception as e:
        logger.error(f"Gagal menulis String JSON ke file Parquet: {e}")
        return False
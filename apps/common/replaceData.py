import pandas as pd
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def replaceData(
    pathSource: str | Path,
    pathTarget: str | Path | None = None,
    keySource: list | None = None,
    fromData: list | None = None,
    toData: list | None = None
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keySource:
        logger.error("KeySource tidak boleh kosong.")
        return False

    if not fromData or not toData:
        logger.error("Parameter 'fromData' atau 'toData' tidak boleh kosong.")
        return False

    if len(fromData) != len(toData):
        logger.error(f"Gagal: Jumlah data from ({len(fromData)}) dan to ({len(toData)}) tidak sama!")
        return False

    try:
        df = None
        
        if isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON. Membaca dari memori...")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                source_obj = Path(pathSource)
                if not source_obj.exists():
                    logger.error(f"File sumber tidak ditemukan: {source_obj}")
                    return False
                logger.info(f"Membaca file untuk proses replace data: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk proses replace data: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        missing_cols = [col for col in keySource if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
        
        replace_mapping = dict(zip(fromData, toData))
        
        for col in keySource:
            df[col] = df[col].replace(replace_mapping)
            
        logger.info(f"Berhasil me-replace value pada kolom {keySource} dengan mapping: {replace_mapping}")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil replace data disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil replace sebagai String JSON.")
            return df.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses replace data JSON: {e}")
        return False
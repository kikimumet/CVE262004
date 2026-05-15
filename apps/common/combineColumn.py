import pandas as pd
from pathlib import Path
import logging
import io
from typing import Optional, Union

logger = logging.getLogger(__name__)

def combineColumn(
    pathSource: Union[str, Path],
    pathTarget: Optional[Union[str, Path]] = None,
    colsToCombine: Optional[list] = None,
    targetCol: Optional[str] = None,
    separator: str = "_"
) -> Union[bool, str]:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not colsToCombine:
        logger.error("Kolom yang akan digabungkan (colsToCombine) tidak boleh kosong.")
        return False

    if not targetCol:
        logger.error("Target kolom (targetCol) tidak boleh kosong.")
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
                logger.info(f"Membaca file untuk digabungkan kolomnya: {source_obj.name}")
                df = pd.read_json(source_obj)
                
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk digabungkan kolomnya: {pathSource.name}")
            df = pd.read_json(pathSource)
            
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        missing_cols = [col for col in colsToCombine if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False
    
        df[targetCol] = df[colsToCombine].astype(str).agg(separator.join, axis=1)
        
        logger.info(f"Berhasil menggabungkan {colsToCombine} menjadi kolom '{targetCol}' dengan separator '{separator}'")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil penggabungan disimpan di: {target_obj}")
            
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan data sebagai String JSON.")
            return df.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses penggabungan JSON: {e}")
        return False
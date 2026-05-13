import pandas as pd
from pathlib import Path
from typing import Union, Optional, List
import logging
import io

logger = logging.getLogger(__name__)

def renameJsonParam(
    pathSource: Union[str, Path],
    pathTarget: Optional[Union[str, Path]] = None,
    keySource: Optional[List[str]] = None,
    keyTarget: Optional[List[str]] = None
) -> Union[bool, str]:

    if not pathSource:
        logger.error("pathSource tidak boleh kosong.")
        return False

    if not keySource or not keyTarget:
        logger.error("KeySource atau KeyTarget tidak boleh kosong.")
        return False

    if len(keySource) != len(keyTarget):
        logger.error(f"Gagal: Jumlah KeySource ({len(keySource)}) dan KeyTarget ({len(keyTarget)}) tidak sama!")
        return False

    try:
        df = None

        if isinstance(pathSource, str):
            cleaned_str = pathSource.strip()

            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON. Membaca dari memori...")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                logger.info(f"Input terdeteksi sebagai String Alamat File: {pathSource}")
                p = Path(pathSource)
                if not p.is_file():
                    logger.error(f"File tidak ditemukan di sistem: {pathSource}")
                    return False
                df = pd.read_json(p)

        elif isinstance(pathSource, Path):
            logger.info(f"Input terdeteksi sebagai Objek Path: {pathSource.name}")
            if not pathSource.is_file():
                logger.error(f"File tidak ditemukan di sistem: {pathSource}")
                return False
            df = pd.read_json(pathSource)

        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        # --- PROSES RENAME ---
        missing_cols = [col for col in keySource if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di data: {missing_cols}")
            return False

        rename_mapping = dict(zip(keySource, keyTarget))
        df = df.rename(columns=rename_mapping)
        logger.info(f"Berhasil mengubah nama kolom: {rename_mapping}")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil rename disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil sebagai String JSON.")
            return df.to_json(orient="records")

    except Exception as e:
        logger.error(f"Gagal memproses rename JSON: {e}")
        return False
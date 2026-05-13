import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def mergeFileParquet(
    pathSource: list | None = None, 
    pathTarget: str | Path | None = None
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource kosong. Harap berikan list file Parquet.")
        return False

    logger.info(f"Memulai proses merge untuk {len(pathSource)} file Parquet...")
    all_dataframes = []

    for file_path in pathSource:
        path_obj = Path(file_path)
        if not path_obj.exists():
            logger.warning(f"File dilewati karena tidak ditemukan: {path_obj}")
            continue
            
        try:
            df = pd.read_parquet(path_obj)
            all_dataframes.append(df)
            logger.info(f"Berhasil membaca Parquet: {path_obj.name}")
        except Exception as e:
            logger.error(f"Gagal memproses file {path_obj.name}: {e}")

    if not all_dataframes:
        logger.error("Tidak ada data Parquet yang valid dari PathSource.")
        return False

    try:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates()
        final_count = len(combined_df)
        
        if initial_count != final_count:
            logger.info(f"Menghapus {initial_count - final_count} baris data duplikat.")
            
    except Exception as e:
        logger.error(f"Gagal menggabungkan data Parquet: {e}")
        return False
    
    if pathTarget:
        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        try:
            combined_df.to_parquet(target_obj, index=False)
            logger.info(f"SUKSES! Data Parquet merge disimpan di: {target_obj}")
            return True
        except Exception as e:
            logger.error(f"Gagal menyimpan file ke PathTarget ({target_obj}): {e}")
            return False
    else:
        logger.info("pathTarget kosong. Mengembalikan Parquet gabungan sebagai String JSON.")
        return combined_df.to_json(orient="records")
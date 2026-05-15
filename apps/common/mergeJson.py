import pandas as pd
from pathlib import Path
import logging
import io
from typing import Optional, Union

logger = logging.getLogger(__name__)

def mergeJson(
    pathSource: Optional[list] = None,
    pathTarget: Optional[Union[str, Path]] = None,
    keySource: Optional[list] = None
) -> Union[bool, str]:

    if not pathSource:
        logger.error("PathSource kosong. Harap berikan list file yang ingin digabungkan.")
        return False

    logger.info(f"Memulai proses merge untuk {len(pathSource)} sumber data...")
    all_dataframes = []

    for source_item in pathSource:
        try:
            df = None
            
            if isinstance(source_item, str):
                cleaned_str = source_item.strip()
                if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                    logger.info("Membaca data dari String JSON memori...")
                    df = pd.read_json(io.StringIO(cleaned_str))
                else:
                    path_obj = Path(source_item)
                    if not path_obj.exists():
                        logger.warning(f"File dilewati karena tidak ditemukan: {path_obj}")
                        continue
                    logger.info(f"Berhasil membaca file: {path_obj.name}")
                    df = pd.read_json(path_obj)
            elif isinstance(source_item, Path):
                if not source_item.exists():
                    logger.warning(f"File dilewati karena tidak ditemukan: {source_item}")
                    continue
                logger.info(f"Berhasil membaca file: {source_item.name}")
                df = pd.read_json(source_item)
            else:
                logger.warning("Tipe source tidak didukung, dilewati.")
                continue

            if keySource:
                available_cols = [col for col in keySource if col in df.columns]
                df = df[available_cols]
                
            all_dataframes.append(df)
            
        except Exception as e:
            logger.error(f"Gagal memproses sumber data: {e}")

    if not all_dataframes:
        logger.error("Tidak ada data JSON valid dari PathSource.")
        return False

    try:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates()
        final_count = len(combined_df)
        
        if initial_count != final_count:
            logger.info(f"Menghapus {initial_count - final_count} baris data duplikat.")
            
    except Exception as e:
        logger.error(f"Gagal menggabungkan Json: {e}")
        return False
    
    if pathTarget:
        target_obj = Path(pathTarget)
        target_obj.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            combined_df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! Data merge berhasil disimpan di: {target_obj}")
            return True
        except Exception as e:
            logger.error(f"Gagal menyimpan file ke PathTarget ({target_obj}): {e}")
            return False
            
    else:
        logger.info("pathTarget kosong. Mengembalikan data gabungan sebagai String JSON.")
        return combined_df.to_json(orient="records")
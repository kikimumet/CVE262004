import pandas as pd
import json
from pathlib import Path
import logging
import io
from typing import Optional, Union

logger = logging.getLogger(__name__)

def forwardFill(
    pathSource: Union[str, Path], 
    pathTarget: Optional[Union[str, Path]] = None, 
    pathPreset: Optional[Union[str, Path, dict]] = None,
    sortCols: Optional[list] = None,
    groupCols: Optional[list] = None,
    dropNulls: bool = True            # Hanya opsi dropNulls yang tersisa
) -> Union[bool, str]:
    
    if sortCols is None: sortCols = ["t"]
    if groupCols is None: groupCols = ["wct", "technum"]

    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    try:
        df = None
        if isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sbg String JSON. Membaca dari memori...")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                source_obj = Path(pathSource)
                if not source_obj.exists():
                    logger.error(f"File sumber tidak ditemukan: {source_obj}")
                    return False
                logger.info(f"Membaca file untuk Forward Fill & Validasi: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk Forward Fill & Validasi: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        # --- BAGIAN 1: PERSIAPAN KOLOM FORWARD FILL ---
        if pathPreset:
            logger.info("Membaca konfigurasi dari Preset baru...")
            preset = {}
            if isinstance(pathPreset, dict):
                preset = pathPreset
            elif isinstance(pathPreset, str) and (pathPreset.strip().startswith('{') or pathPreset.strip().startswith('[')):
                preset = json.loads(pathPreset)
            else:
                p_preset = Path(pathPreset)
                if not p_preset.exists():
                    logger.error(f"File preset tidak ditemukan: {p_preset}")
                    return False
                with open(p_preset, 'r') as f:
                    preset = json.load(f)
                    
            detail_params = preset.get("detailParam", [])
            wct_val = "UNKNOWN"
            tech_val = "UNKNOWN"
            param_ids = set()

            for item in detail_params:
                pid = item.get("paramid")
                if pid:
                    param_ids.add(pid)
                    if wct_val == "UNKNOWN":
                        wct_val = item.get("wctid", "UNKNOWN")
                        tech_val = item.get("technum", "UNKNOWN")

            cols_to_ffill = []
            for param_name in param_ids:
                nval_col = f"nvalue_{wct_val}_{tech_val}_{param_name}"
                cat_col = f"category_{wct_val}_{tech_val}_{param_name}"
                thresh_col = f"threshold_{wct_val}_{tech_val}_{param_name}"
                
                if nval_col in df.columns: cols_to_ffill.append(nval_col)
                if cat_col in df.columns: cols_to_ffill.append(cat_col)
                if thresh_col in df.columns: cols_to_ffill.append(thresh_col)
                    
        else:
            logger.info("Preset tidak diberikan. FFill ke seluruh kolom kecuali sorting & grouping...")
            cols_to_ffill = [col for col in df.columns if col not in sortCols and col not in groupCols]

        # --- BAGIAN 2: EKSEKUSI FORWARD FILL ---
        sort_by = [c for c in sortCols if c in df.columns]
        if sort_by:
            primary_sort = sort_by[0]
            if pd.api.types.is_numeric_dtype(df[primary_sort]):
                df = df.sort_values(by=sort_by)
            else:
                df['__temp_time__'] = pd.to_datetime(df[primary_sort], errors='coerce')
                df = df.sort_values(by=['__temp_time__'] + sort_by[1:]).drop(columns=['__temp_time__'])

        valid_group_cols = [col for col in groupCols if col in df.columns]
        
        if valid_group_cols and cols_to_ffill:
            logger.info(f"Melakukan Forward Fill pada {len(cols_to_ffill)} kolom berdasarkan grup.")
            df[cols_to_ffill] = df.groupby(valid_group_cols)[cols_to_ffill].ffill()
        elif cols_to_ffill:
            logger.info(f"Melakukan Forward Fill global pada {len(cols_to_ffill)} kolom.")
            df[cols_to_ffill] = df[cols_to_ffill].ffill()
        else:
            logger.warning("Tidak ada kolom yang dieksekusi untuk forward fill.")

        # --- BAGIAN 3: VALIDATOR (PENGHAPUSAN SELURUH NULL) ---
        if dropNulls:
            initial_row_count = len(df)
            
            # Langsung drop semua baris yang memiliki Null tanpa pandang bulu
            df = df.dropna().copy()
                
            final_row_count = len(df)
            dropped_count = initial_row_count - final_row_count
            logger.info(f"Validator Aktif: Menghapus {dropped_count} baris yang masih Null setelah proses FFill.")

        # --- BAGIAN 4: MENYIMPAN HASIL ---
        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil disimpan di: {target_obj}")
            return True
        else:
            return df.to_json(orient="records")
            
    except Exception as e:
        logger.error(f"Gagal memproses Forward Fill & Validator: {e}")
        return False
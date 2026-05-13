import pandas as pd
import json
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def outlier(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    pathPreset: str | Path | dict | None = None,
    groupCols: list | None = None,
    keyValue: str = "nvalue"
) -> bool | str:
    
    if groupCols is None:
        groupCols = ["wct", "technum", "param"]

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
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        is_active = True
        if pathPreset:
            logger.info("Mengecek konfigurasi outlier dari Preset...")
            preset = {}
            if isinstance(pathPreset, dict):
                preset = pathPreset
            elif isinstance(pathPreset, str) and (pathPreset.strip().startswith('{') or pathPreset.strip().startswith('[')):
                preset = json.loads(pathPreset)
            else:
                p_preset = Path(pathPreset)
                if p_preset.exists():
                    with open(p_preset, 'r') as f:
                        preset = json.load(f)
            
            global_settings = preset.get("global_settings", [])
            is_active = False
            for item in global_settings:
                if item.get("key") == "outlier" and str(item.get("value")).lower() == "true":
                    is_active = True
                    break

        if not is_active:
            logger.info("Outlier dinonaktifkan di preset. Melewati proses...")
            df_result = df.copy()
        else:
            logger.info("Menjalankan penghapusan Outlier (Metode IQR)...")
            initial_count = len(df)
            df[keyValue] = pd.to_numeric(df[keyValue], errors='coerce')

            def iqr_mask(x):
                valid_x = x.dropna()
                if valid_x.empty: return pd.Series(True, index=x.index)
                
                q1 = valid_x.quantile(0.25)
                q3 = valid_x.quantile(0.75)
                iqr = q3 - q1
                return (x >= q1 - 1.5 * iqr) & (x <= q3 + 1.5 * iqr) | x.isna()

            valid_group_cols = [col for col in groupCols if col in df.columns]
            
            if valid_group_cols:
                logger.info(f"Menghitung IQR spesifik per grup: {valid_group_cols}")
                mask = df.groupby(valid_group_cols)[keyValue].transform(iqr_mask)
            else:
                logger.warning("Kolom grup tidak ditemukan di data. Menghitung IQR secara global!")
                mask = iqr_mask(df[keyValue])
            
            df_result = df[mask.astype(bool)].copy()
            
            dropped = initial_count - len(df_result)
            logger.info(f"Proses Outlier Selesai. {dropped} baris data anomali dihapus.")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_result.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil outlier disimpan di: {target_obj}")
            return True
        else:
            return df_result.to_json(orient="records")
            
    except Exception as e:
        logger.error(f"Gagal memproses Outlier: {e}")
        return False
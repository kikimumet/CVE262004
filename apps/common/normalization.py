import pandas as pd
import json
from pathlib import Path
import logging
import io
from typing import Optional, Union

logger = logging.getLogger(__name__)

def normalization(
    pathSource: Union[str, Path, pd.DataFrame], 
    pathTarget: Optional[Union[str, Path]] = None,
    pathPreset: Optional[Union[str, Path, dict]] = None,
    keyValue: str = "nvalue", 
    keyWct: str = "wct",
    keyTech: str = "technum",
    keyParam: str = "param"
) -> Union[bool, str, pd.DataFrame]:
    
    if pathPreset is None:
        logger.error("PathPreset tidak boleh kosong.")
        return False
        
    try:
        preset_data = {}
        if isinstance(pathPreset, dict):
            preset_data = pathPreset
        elif isinstance(pathPreset, str) and (pathPreset.strip().startswith('{') or pathPreset.strip().startswith('[')):
            preset_data = json.loads(pathPreset)
        else:
            preset_obj = Path(pathPreset)
            if not preset_obj.exists():
                logger.error(f"File preset tidak ditemukan: {preset_obj}")
                return False
            with open(preset_obj, 'r') as f:
                preset_data = json.load(f)
                
        detail_params = preset_data.get("detailParam", [])
        if not detail_params:
            logger.error("Tidak ada data 'detailParam' di dalam file preset.")
            return False

        min_dict = {}
        max_dict = {}
        
        for item in detail_params:
            wct_id = item.get("wctid")
            tech_num = item.get("technum")
            param_id = item.get("paramid")
            setting_key = item.get("key")
            setting_value = item.get("value")
            
            if not wct_id or not tech_num or not param_id or not setting_key or setting_value is None:
                continue

            composite_key = f"{wct_id}_{tech_num}_{param_id}"

            try:
                num_value = float(setting_value)
                if setting_key == "rangeMin":
                    min_dict[composite_key] = num_value
                elif setting_key == "rangeMax":
                    max_dict[composite_key] = num_value
            except ValueError:
                pass

        df = None
        if isinstance(pathSource, pd.DataFrame):
            df = pathSource.copy()
        elif isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON. Membaca dari memori...")
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

        required_cols = [keyValue, keyWct, keyTech, keyParam]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di data: {missing_cols}")
            return False

        df['__temp_composite'] = df[keyWct].astype(str) + "_" + df[keyTech].astype(str) + "_" + df[keyParam].astype(str)

        df['__temp_min'] = df['__temp_composite'].map(min_dict)
        df['__temp_max'] = df['__temp_composite'].map(max_dict)

        df[keyValue] = pd.to_numeric(df[keyValue], errors="coerce")
        df['__temp_min'] = pd.to_numeric(df['__temp_min'], errors="coerce")
        df['__temp_max'] = pd.to_numeric(df['__temp_max'], errors="coerce")
            
        range_diff = df['__temp_max'] - df['__temp_min']
        
        mask = (range_diff > 0) & df['__temp_min'].notnull() & df['__temp_max'].notnull() & df[keyValue].notnull()
        
        df.loc[mask, keyValue] = (df.loc[mask, keyValue] - df.loc[mask, '__temp_min']) / range_diff[mask]
        
        df = df.drop(columns=['__temp_composite', '__temp_min', '__temp_max'])
        
        logger.info("Min-Max Normalization Absolute via Preset berhasil diterapkan.")
        
        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil normalisasi disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil normalisasi sebagai String JSON.")
            return df.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Error saat menjalankan fungsi Normalisasi via Preset: {e}")
        return False
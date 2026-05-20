import pandas as pd
from pathlib import Path
import logging
import io
from typing import Optional, Union

logger = logging.getLogger(__name__)

def aggregateTime(
    pathSource: Union[str, Path],
    pathTarget: Optional[Union[str, Path]] = None,
    keyTime: str = "t",
    keyValues: Optional[list] = None,
    aggMethods: Optional[list] = None,
    keyGroups: Optional[list] = None
) -> Union[bool, str]:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keyValues or not aggMethods:
        logger.error("keyValues dan aggMethods tidak boleh kosong.")
        return False
        
    if len(keyValues) != len(aggMethods):
        logger.error("Jumlah elemen pada keyValues dan aggMethods harus sama!")
        return False

    if keyGroups is None:
        keyGroups = []

    try:
        df = None
        
        if isinstance(pathSource, str):
            cleaned_str = pathSource.strip()
            if cleaned_str.startswith('{') or cleaned_str.startswith('['):
                logger.info("Input terdeteksi sebagai String JSON.")
                df = pd.read_json(io.StringIO(cleaned_str))
            else:
                source_obj = Path(pathSource)
                if not source_obj.exists():
                    return False
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                return False
            df = pd.read_json(pathSource)
        else:
            return False

        if keyTime not in df.columns:
            logger.error(f"Kolom waktu '{keyTime}' tidak ditemukan di dataset.")
            return False

        original_cols = df.columns.tolist()

        if pd.api.types.is_numeric_dtype(df[keyTime]):
            df = df.sort_values(by=keyTime)
        else:
            df['__temp_time__'] = pd.to_datetime(df[keyTime], errors='coerce')
            df = df.sort_values(by='__temp_time__').drop(columns=['__temp_time__'])

        groupby_cols = keyGroups + [keyTime]
        groupby_cols = [c for c in groupby_cols if c in df.columns]

        agg_dict = {}
        for col, method in zip(keyValues, aggMethods):
            if col in df.columns:
                m = method.lower()
                if m in 'mean':
                    agg_dict[col] = 'mean'
                elif m == 'min':
                    agg_dict[col] = 'min'
                elif m == 'max':
                    agg_dict[col] = 'max'
                elif m in 'last':
                    agg_dict[col] = 'last'
                else:
                    agg_dict[col] = 'last'

        for col in df.columns:
            if col not in groupby_cols and col not in agg_dict:
                agg_dict[col] = 'last' 

        df_agg = df.groupby(groupby_cols, as_index=False).agg(agg_dict)
        
        final_cols = [col for col in original_cols if col in df_agg.columns]
        df_agg = df_agg[final_cols]

        logger.info(f"Berhasil agregasi data. Menghasilkan {len(df_agg)} baris.")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_agg.to_json(target_obj, orient="records", indent=4)
            return True
        else:
            return df_agg.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses agregasi waktu JSON: {e}")
        return False
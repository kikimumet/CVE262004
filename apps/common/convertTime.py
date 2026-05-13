import pandas as pd
from pathlib import Path
from typing import Union, Optional, List
import logging
import io

logger = logging.getLogger(__name__)

def convertTime(
    pathSource: Union[str, Path],
    pathTarget: Optional[Union[str, Path]] = None,
    keySource: Optional[List[str]] = None,
    formatTime: Optional[str] = None
) -> Union[bool, str]:

    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keySource:
        logger.error("KeySource tidak boleh kosong.")
        return False

    if not formatTime:
        logger.error("FormatTime tidak boleh kosong.")
        return False

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

        missing_cols = [col for col in keySource if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom berikut tidak ditemukan di source: {missing_cols}")
            return False

        def safe_epoch_convert(val, is_ms=False):
            if pd.isnull(val):
                return None
            if val.tzinfo is None:
                val = val.tz_localize('+07:00')
            if is_ms:
                return int(val.timestamp() * 1000)
            else:
                return int(val.timestamp())

        for col in keySource:
            dt_series = pd.to_datetime(df[col], errors='coerce')

            if formatTime == "epoch_ms":
                df[col] = dt_series.apply(lambda x: safe_epoch_convert(x, is_ms=True))
            elif formatTime == "epoch":
                df[col] = dt_series.apply(lambda x: safe_epoch_convert(x, is_ms=False))
            else:
                def to_string_tz(val):
                    if pd.isnull(val): return None
                    if val.tzinfo is None:
                        return val.strftime(formatTime)
                    else:
                        return val.tz_convert('+07:00').tz_localize(None).strftime(formatTime)

                df[col] = dt_series.apply(to_string_tz)

        if formatTime in ("epoch", "epoch_ms"):
            for col in keySource:
                df[col] = df[col].astype("Int64")

        logger.info(f"Berhasil mengubah format waktu pada kolom {keySource} menjadi '{formatTime}'")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            return True
        else:
            return df.to_json(orient="records")

    except Exception as e:
        logger.error(f"Gagal memproses convert waktu JSON: {e}")
        return False
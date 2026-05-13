import pandas as pd
from pathlib import Path
import logging
from typing import Literal
import io

logger = logging.getLogger(__name__)

def filterTime(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    keyStart: str | None = None, 
    keyEnd: str | None = None, 
    maxDelta: int | float | None = None, 
    unitDelta: Literal['days', 'day', 'hours', 'hour', 'minutes', 'minute', 'seconds', 'second', 'W', 'D', 'h', 'm', 's'] | None = None
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keyStart or not keyEnd:
        logger.error("KeyStart atau KeyEnd tidak boleh kosong.")
        return False

    try:
        try:
            if maxDelta is None or unitDelta is None:
                logger.error("maxDelta dan unitDelta tidak boleh kosong.")
                return False
            max_td = pd.Timedelta(value=maxDelta, unit=unitDelta)
        except Exception as e:
            logger.error(f"Gagal mendefinisikan Timedelta. Pastikan parameter valid: {e}")
            return False

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
                logger.info(f"Membaca file untuk proses filter time delta: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk proses filter time delta: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        if keyStart not in df.columns or keyEnd not in df.columns:
            logger.error(f"Kolom '{keyStart}' atau '{keyEnd}' tidak ditemukan di dataset.")
            return False

        initial_row_count = len(df)

        def parse_time(series):
            if pd.api.types.is_numeric_dtype(series):
                if series.max() > 1e11:
                    return pd.to_datetime(series, unit='ms', errors='coerce')
                else:
                    return pd.to_datetime(series, unit='s', errors='coerce')
            else:
                return pd.to_datetime(series, errors='coerce', dayfirst=True)

        dt_start = parse_time(df[keyStart])
        dt_end = parse_time(df[keyEnd])

        cond_valid_date = dt_start.notna() & dt_end.notna()
        cond_not_future = dt_start <= dt_end
        cond_max_delta = (dt_end - dt_start) <= max_td
    
        df_filtered = df[cond_valid_date & cond_not_future & cond_max_delta].copy()
        
        final_row_count = len(df_filtered)
        dropped_count = initial_row_count - final_row_count
        
        logger.info(f"Filter Selesai. Menghapus {dropped_count} baris yang tidak memenuhi syarat/berformat salah.")

        if final_row_count == 0:
            logger.warning("Peringatan: Seluruh data terhapus oleh filter. Data JSON akan kosong.")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_filtered.to_json(target_obj, orient="records", indent=4, date_format="iso")
            logger.info(f"SUKSES! File hasil filter time delta disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil filter sebagai String JSON.")
            return df_filtered.to_json(orient="records", date_format="iso")
        
    except Exception as e:
        logger.error(f"Gagal memproses filter time delta JSON: {e}")
        return False
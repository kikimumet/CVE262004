import pandas as pd
import json
from pathlib import Path
import logging
import io

logger = logging.getLogger(__name__)

def pivotTable(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    pathPreset: str | Path | dict | None = None,
    indexCols: list | None = None, 
    pivotCol: str = 'param', 
    valueCol: str | list | None = None,
    aggFunc: str = 'first',
    timeCol: str | None = None,
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not indexCols or not pivotCol:
        logger.error("Parameter indexCols dan pivotCol tidak boleh kosong.")
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
                logger.info(f"Membaca file untuk proses Pivot: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk proses Pivot: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        for col in indexCols:
            if col in df.columns:
                df[col] = df[col].fillna("N/A")

        if pathPreset:
            possible_values = ['nvalue', 'category', 'threshold']
            valueCols = [col for col in possible_values if col in df.columns]
        else:
            if not valueCol:
                logger.error("Parameter valueCol wajib diisi jika tidak menggunakan preset.")
                return False
            valueCols = [valueCol] if isinstance(valueCol, str) else valueCol

        logger.info("Melakukan proses pivot table...")
        df_pivoted = df.pivot_table(
            index=indexCols, 
            columns=pivotCol, 
            values=valueCols, 
            aggfunc=aggFunc # type: ignore
        )

        if pathPreset:
            logger.info("Menerapkan konfigurasi dari Preset baru...")
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
            preset_params = {}

            for item in detail_params:
                pid = item.get("paramid")
                if not pid: continue
                
                if wct_val == "UNKNOWN":
                    wct_val = item.get("wctid", "UNKNOWN")
                    tech_val = item.get("technum", "UNKNOWN")
                    
                if pid not in preset_params:
                    preset_params[pid] = {}
                    
                k = item.get("key")
                v = item.get("value")
                
                if k in ["category", "threshold"]:
                    preset_params[pid][k] = (str(v).lower() == "true")

            df_result = df_pivoted.index.to_frame(index=False)
            nvalue_cols, critical_cols, threshold_cols = [], [], []

            for param_name, param_config in preset_params.items():
                if ('nvalue', param_name) in df_pivoted.columns:
                    nval_col = f"nvalue_{wct_val}_{tech_val}_{param_name}"
                    df_result[nval_col] = df_pivoted[('nvalue', param_name)].values
                    nvalue_cols.append(nval_col)
                
                if param_config.get('category') is True and ('category', param_name) in df_pivoted.columns:
                    crit_col = f"category_{wct_val}_{tech_val}_{param_name}" # Menggunakan nama category agar rapi
                    df_result[crit_col] = df_pivoted[('category', param_name)].values
                    critical_cols.append(crit_col)
                    
                if param_config.get('threshold') is True and ('threshold', param_name) in df_pivoted.columns:
                    thresh_col = f"threshold_{wct_val}_{tech_val}_{param_name}"
                    df_result[thresh_col] = df_pivoted[('threshold', param_name)].values
                    threshold_cols.append(thresh_col)
            
            ordered_cols = indexCols + nvalue_cols + critical_cols + threshold_cols
            df_result = df_result[[col for col in ordered_cols if col in df_result.columns]]
            
        else:
            logger.info("Menerapkan format penamaan standar...")
            if isinstance(df_pivoted.columns, pd.MultiIndex):
                df_pivoted.columns = [f"{col[0]}_{col[1]}" if pd.notna(col[1]) and col[1] != '' else col[0] for col in df_pivoted.columns.values]
            
            df_result = df_pivoted.reset_index()
            df_result.columns.name = None

        if timeCol and timeCol in df_result.columns:
            if pd.api.types.is_numeric_dtype(df_result[timeCol]):
                df_result = df_result.sort_values(by=timeCol, ascending=True)
            else:
                df_result['__temp_time__'] = pd.to_datetime(df_result[timeCol], errors='coerce')
                df_result = df_result.sort_values(by='__temp_time__', ascending=True).drop(columns=['__temp_time__'])

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df_result.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil pivot disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil pivot sbg String JSON.")
            return df_result.to_json(orient="records")
            
    except Exception as e:
        logger.error(f"Gagal memproses pivot tabel: {e}")
        return False
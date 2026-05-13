import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Any
import io

logger = logging.getLogger(__name__)

def compareValue(
    pathSource: str | Path, 
    pathTarget: str | Path | None = None, 
    keySource: str | None = None, 
    keyCompare: list | None = None, 
    operators: list | None = None, 
    resultCompare: list | None = None, 
    defaultValue: Any = None,
    keyResult: str | None = None
) -> bool | str:
    
    if not pathSource:
        logger.error("PathSource tidak boleh kosong.")
        return False

    if not keySource or not keyResult:
        logger.error("KeySource atau KeyResult tidak boleh kosong.")
        return False

    if keyCompare is None or operators is None or resultCompare is None:
        logger.error("KeyCompare, Operator, dan ResultComparation tidak boleh kosong.")
        return False

    if not (len(keyCompare) == len(operators) == len(resultCompare)):
        logger.error("Gagal: Jumlah KeyComparation, Operator, dan ResultComparation harus sama persis!")
        return False

    try:
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
                logger.info(f"Membaca file untuk proses komparasi nilai: {source_obj.name}")
                df = pd.read_json(source_obj)
        elif isinstance(pathSource, Path):
            if not pathSource.is_file():
                logger.error(f"File sumber tidak ditemukan: {pathSource}")
                return False
            logger.info(f"Membaca file untuk proses komparasi nilai: {pathSource.name}")
            df = pd.read_json(pathSource)
        else:
            logger.error("Tipe input pathSource tidak didukung!")
            return False

        if keySource not in df.columns:
            logger.error(f"Kolom KeySource '{keySource}' tidak ditemukan di dataset.")
            return False

        missing_cols = [col for col in keyCompare if col not in df.columns]
        if missing_cols:
            logger.error(f"Kolom pembanding berikut tidak ditemukan: {missing_cols}")
            return False
        
        conditions = []
        for col, op in zip(keyCompare, operators):
            if op == '<':
                conditions.append(df[keySource] < df[col])
            elif op == '<=':
                conditions.append(df[keySource] <= df[col])
            elif op == '>':
                conditions.append(df[keySource] > df[col])
            elif op == '>=':
                conditions.append(df[keySource] >= df[col])
            elif op == '==':
                conditions.append(df[keySource] == df[col])
            elif op == '!=':
                conditions.append(df[keySource] != df[col])
            else:
                logger.error(f"Operator tidak valid: {op}")
                return False
            
        result_compare = list(resultCompare)
        df[keyResult] = np.select(conditions, result_compare, default=defaultValue)
            
        logger.info(f"Berhasil membandingkan {keySource} dengan {keyCompare} dan membuat kolom baru '{keyResult}'")

        if pathTarget:
            target_obj = Path(pathTarget)
            target_obj.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(target_obj, orient="records", indent=4)
            logger.info(f"SUKSES! File hasil komparasi disimpan di: {target_obj}")
            return True
        else:
            logger.info("pathTarget kosong. Mengembalikan hasil komparasi sebagai String JSON.")
            return df.to_json(orient="records")
        
    except Exception as e:
        logger.error(f"Gagal memproses komparasi JSON: {e}")
        return False
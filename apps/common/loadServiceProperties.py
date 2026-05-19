import os
import logging
from pathlib import Path
from typing import Optional
from apps.common.loadProperties import loadProperties

logger = logging.getLogger(__name__)

# Mapping
_STATUS_SUFFIX = {
    "dev": "_dev",
    "qa":  "_qa",
    "opr": ""
}

def loadServiceProperties(service_name: str, service_dir: Path) -> dict:
    
    server_status = os.getenv("SERVER_STATUS", "dev").lower().strip()

    if server_status not in _STATUS_SUFFIX:
        raise RuntimeError(
            f"[{service_name}] SERVER_STATUS='{server_status}' tidak valid. "
            f"Gunakan salah satu: {list(_STATUS_SUFFIX.keys())}"
        )

    suffix = _STATUS_SUFFIX[server_status]
    filename = f"{service_name}{suffix}.properties"
    properties_path = service_dir / filename

    logger.info(f"[{service_name}] SERVER_STATUS={server_status} → loading '{filename}'")

    if not properties_path.exists():
        raise RuntimeError(
            f"[{service_name}] File properties tidak ditemukan: {properties_path}"
        )

    cfg = loadProperties(pathConfig=properties_path)
    if not isinstance(cfg, dict):
        raise RuntimeError(
            f"[{service_name}] Gagal membaca file properties: {properties_path}"
        )

    logger.info(f"[{service_name}] Berhasil load {len(cfg)} config dari '{filename}'")
    return cfg
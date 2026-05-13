import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def writeStringJsonToFileJson(stringJson: str, pathTarget: str | Path) -> bool:
    if not stringJson or not pathTarget:
        logger.error("String JSON atau PathTarget tidak boleh kosong.")
        return False

    target_obj = Path(pathTarget)
    target_obj.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Menulis String JSON ke file fisik...")
        records = json.loads(stringJson.strip())
        target_obj.write_text(
            json.dumps(records, ensure_ascii=False, indent=4),
            encoding="utf-8"
        )
        logger.info(f"SUKSES! File JSON berhasil dibuat di: {target_obj}")
        return True
    except Exception as e:
        logger.error(f"Gagal menulis String JSON ke file JSON: {e}")
        return False
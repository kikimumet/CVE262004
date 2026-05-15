import requests
import logging
import json
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

def callApi(
    url: str,
    payload: Union[list, dict, str],
    headers: Optional[dict] = None,
    timeout: int = 30
) -> Optional[list]:

    if not url:
        logger.error("URL tidak boleh kosong.")
        return None

    if not payload:
        logger.error("Payload tidak boleh kosong.")
        return None

    try:
        if isinstance(payload, str):
            payload = json.loads(payload.strip())

        if isinstance(payload, list):
            wrapped = {"data": payload}
        elif isinstance(payload, dict):
            wrapped = payload
        else:
            logger.error("Payload harus berupa list atau dict.")
            return None

        default_headers = {"Content-Type": "application/json"}
        if headers:
            default_headers.update(headers)

        data_len = len(wrapped.get("data", []))
        logger.info(f"Hitting API: {url}")
        logger.info(f"Jumlah data dikirim: {data_len} baris")

        response = requests.post(
            url=url,
            json=wrapped,
            headers=default_headers,
            timeout=timeout
        )

        response.raise_for_status()

        result = response.json()

        if isinstance(result, list):
            logger.info(f"API berhasil. Menerima {len(result)} baris.")
            return result
        elif isinstance(result, dict):
            for key in ["data", "result", "results", "output"]:
                if key in result and isinstance(result[key], list):
                    logger.info(f"API berhasil. Menerima {len(result[key])} baris dari key '{key}'.")
                    return result[key]
            logger.warning("Response API berbentuk dict tapi tidak ada key data yang dikenal. Wrap sebagai list.")
            return [result]
        else:
            logger.error(f"Format response tidak dikenal: {type(result)}")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"Request timeout setelah {timeout} detik.")
        return None
    except requests.exceptions.ConnectionError:
        logger.error(f"Gagal koneksi ke API: {url}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        return None
    except json.JSONDecodeError:
        logger.error("Gagal parse response API sebagai JSON.")
        return None
    except Exception as e:
        logger.error(f"Error tidak terduga saat hit API: {e}")
        return None
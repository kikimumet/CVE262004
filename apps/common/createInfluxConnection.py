import os
import logging
from typing import Optional
from influxdb import InfluxDBClient
from apps.common.decryptData import decryptData

logger = logging.getLogger(__name__)


def createInfluxConnection(
    fernetKey: Optional[str] = None
) -> Optional[InfluxDBClient]:

    host     = os.getenv("INFLUX_HOST", "")
    port     = os.getenv("INFLUX_PORT", "8086")
    username = os.getenv("INFLUX_USERNAME", "")
    password = os.getenv("INFLUX_PASSWORD", "")
    database = os.getenv("INFLUX_DATABASE", "")

    if not all([host, port, username, password, database]):
        logger.error(
            "Konfigurasi InfluxDB tidak lengkap. "
            "Pastikan INFLUX_HOST, INFLUX_PORT, INFLUX_USERNAME, "
            "INFLUX_PASSWORD, dan INFLUX_DATABASE terisi di .env."
        )
        return None

    if fernetKey:
        logger.info("Mendekripsi password InfluxDB...")
        decrypted = decryptData(key=fernetKey, encryptText=password)
        if not isinstance(decrypted, str):
            logger.error("Gagal mendekripsi password InfluxDB.")
            return None
        password = decrypted

    try:
        logger.info(
            f"Mencoba koneksi ke InfluxDB: {host}:{port} "
            f"database '{database}'..."
        )

        client = InfluxDBClient(
            host=host,
            port=int(port),
            username=username,
            password=password,
            database=database
        )

        client.ping()

        logger.info(
            f"Koneksi InfluxDB BERHASIL! "
            f"Database aktif: '{database}'"
        )

        return client

    except Exception as e:
        logger.error(f"Gagal koneksi ke InfluxDB: {e}")
        return None
from influxdb import InfluxDBClient
import logging
from typing import Optional
from apps.common.loadProperties import loadProperties
from apps.common.decryptData import decryptData

logger = logging.getLogger(__name__)


def createInfluxConnection(
    pathConfig: Optional[str] = None,
    fernetKey: Optional[str] = None
) -> Optional[InfluxDBClient]:
    
    if not pathConfig:
        logger.error("PathConfig tidak boleh kosong.")
        return None

    props = loadProperties(pathConfig=pathConfig)
    if not isinstance(props, dict):
        logger.error("Gagal membaca file properties InfluxDB.")
        return None

    host = props.get("influx.host", "")
    port = props.get("influx.port", "8086")
    username = props.get("influx.username", "")
    password = props.get("influx.password", "")
    database = props.get("influx.database", "")

    if not all([host, port, username, password, database]):
        logger.error(
            "Konfigurasi InfluxDB tidak lengkap. "
            "Pastikan influx.host, influx.port, influx.username, "
            "influx.password, dan influx.database terisi."
        )
        return None

    if fernetKey:
        logger.info("Mendekripsi password InfluxDB...")
        decrypted = decryptData(
            key=fernetKey,
            encryptText=password
        )

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
import re
import oracledb
import logging
from pathlib import Path
from typing import Optional, Tuple
from apps.common.loadProperties import loadProperties
from apps.common.decryptData import decryptData

logger = logging.getLogger(__name__)

ORACLE_INSTANT_CLIENT_DIR = "/opt/oracle/instantclient_21_21"

try:
    oracledb.init_oracle_client(lib_dir=ORACLE_INSTANT_CLIENT_DIR)
    logger.info(f"Oracle thick mode aktif: {ORACLE_INSTANT_CLIENT_DIR}")
except oracledb.ProgrammingError as e:
    if "already been called" in str(e).lower():
        pass
    else:
        logger.warning(f"Gagal init Oracle thick mode: {e}")


def parseJdbcUrl(jdbc_url: str) -> Optional[Tuple[str, str, str]]:
    pattern = r"jdbc:oracle:thin:@([^:]+):(\d+)[:/](.+)"
    match = re.match(pattern, jdbc_url.strip())
    if not match:
        logger.error(f"Format jdbc.url tidak valid: {jdbc_url}")
        return None
    return match.group(1), match.group(2), match.group(3)


def createOracleConnection(
    pathConfig: Optional[str] = None,
    fernetKey: Optional[str] = None
) -> Optional[oracledb.Connection]:

    if not pathConfig:
        logger.error("PathConfig tidak boleh kosong.")
        return None

    props = loadProperties(pathConfig=pathConfig)
    if not isinstance(props, dict):
        logger.error("Gagal membaca file properties Oracle.")
        return None

    jdbc_url = props.get("jdbc.url", "")
    username = props.get("jdbc.username", "")
    password = props.get("jdbc.password", "")

    if not all([jdbc_url, username, password]):
        logger.error("Konfigurasi Oracle tidak lengkap (jdbc.url / jdbc.username / jdbc.password).")
        return None

    parsed = parseJdbcUrl(jdbc_url)
    if not parsed:
        return None
    host, port, sid = parsed

    if fernetKey:
        logger.info("Mendekripsi password Oracle...")
        decrypted = decryptData(key=fernetKey, encryptText=password)
        if not isinstance(decrypted, str):
            logger.error("Gagal mendekripsi password Oracle.")
            return None
        password = decrypted

    dsn = f"{host}:{port}/{sid}"

    try:
        logger.info(f"Mencoba koneksi ke Oracle: {dsn} sebagai '{username}'...")
        connection = oracledb.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        logger.info("Koneksi Oracle BERHASIL!")
        return connection

    except oracledb.DatabaseError as e:
        logger.error(f"Gagal koneksi ke Oracle: {e}")
        return None
    except Exception as e:
        logger.error(f"Error tidak terduga saat koneksi Oracle: {e}")
        return None
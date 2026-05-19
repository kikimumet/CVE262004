import json
import math
import time
import logging
import os
from datetime import datetime
from collections import Counter
from pathlib import Path
from typing import Optional, Tuple, Set
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

from apps.config import (
    KAFKA_TOPIC,
    KAFKA_SERVERS,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SSL_CAFILE,
)

from apps.common.callApi import callApi
from apps.common.saveAppender import saveAndAppend
from apps.common.createInfluxConnection import createInfluxConnection
from apps.common.createOracleConnection import createOracleConnection
from apps.common.renameJsonParam import renameJsonParam
from apps.common.convertTime import convertTime
from apps.common.loadServiceProperties import loadServiceProperties
from apps.logger import setupLogger

# LOAD CONFIG FROM PROPERTIES
_SERVICE_NAME = "ahmmoprd002"
_SERVICE_DIR  = Path(__file__).parent

_cfg = loadServiceProperties(
    service_name=_SERVICE_NAME,
    service_dir=_SERVICE_DIR
)

KAFKA_GROUP_ID     = _cfg.get("kafka.group_id", "")
RAPIDMINER_TIMEOUT = int(_cfg.get("rapidminer_timeout", 60))
SAVE_DIR_RAW       = _cfg.get("save_dir_raw", "")
INFLUX_MEASUREMENT = _cfg.get("influx.measurement", "")
ORACLE_INSIGHT_NAME = _cfg.get("oracle.insight_name", "")
SHIFT_TRIGGER_TIMES   = [t.strip() for t in _cfg.get("shift_trigger_times", "17:00,01:00,08:00").split(",") if t.strip()]
SHIFT_TRIGGER_LABELS   = [l.strip() for l in _cfg.get("shift_trigger_labels", "").split(",") if l.strip()]

USE_COLUMN        = [c.strip() for c in _cfg.get("use_column", "").split(",") if c.strip()]
KEY_SOURCE_RENAME = [c.strip() for c in _cfg.get("key_source_rename", "").split(",") if c.strip()]
KEY_TARGET_RENAME = [c.strip() for c in _cfg.get("key_target_rename", "").split(",") if c.strip()]

FERNET_KEY = os.getenv("FERNET_KEY", "")

# LOGGER
setupLogger(level=logging.INFO, log_dir="apps/logs")
logger = logging.getLogger(_SERVICE_NAME)

# VALIDATION
_REQUIRED_CFG = {
    "kafka.group_id": KAFKA_GROUP_ID,
    "save_dir_raw": SAVE_DIR_RAW,
    "influx.measurement": INFLUX_MEASUREMENT,
    "oracle.insight_name": ORACLE_INSIGHT_NAME,
    "shift_trigger_times": SHIFT_TRIGGER_TIMES,
    "use_column": USE_COLUMN,
    "key_source_rename": KEY_SOURCE_RENAME,
    "key_target_rename": KEY_TARGET_RENAME,
}
for _k, _v in _REQUIRED_CFG.items():
    if not _v:
        raise RuntimeError(f"[{_SERVICE_NAME}] Config '{_k}' kosong di properties file.")

if len(KEY_SOURCE_RENAME) != len(KEY_TARGET_RENAME):
    raise RuntimeError(f"[{_SERVICE_NAME}] Jumlah key_source_rename dan key_target_rename harus sama.")

# LEFTOVER BUFFER
leftover_buffer = []

# SHIFT TRIGGER CHECKER
def is_shift_trigger(last_executed_shift: str) -> Tuple[bool, str]:
    now = datetime.now()
    current_hhmm = now.strftime("%H:%M")

    if current_hhmm in SHIFT_TRIGGER_TIMES:
        if last_executed_shift != current_hhmm:
            return True, current_hhmm

    return False, last_executed_shift


def get_current_shift_label(trigger_time: str) -> str:
    shift_map = dict(zip(SHIFT_TRIGGER_TIMES, SHIFT_TRIGGER_LABELS))
    return shift_map.get(trigger_time, f"Shift trigger {trigger_time}")

# LOAD FILTER FROM ORACLE
def load_filter_from_oracle() -> Optional[Tuple[str, str, Set[str], str]]:
    conn = createOracleConnection(fernetKey=FERNET_KEY)
    if conn is None:
        logger.error(f"[{_SERVICE_NAME}] Koneksi Oracle gagal. Stream dihentikan.")
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT B.VPARAMID, B.VWCTID, B.VTECHNUM, A.VURLAPI
            FROM AHMMODCM_HDRINSGHTS A
            LEFT JOIN AHMMODCM_DTLINSGHTS B ON A.VINSNAME = B.VINSNAME AND A.VWCTID = B.VWCTID
            LEFT JOIN AHMMODCM_DTLINSPRES C ON A.VINSNAME = C.VINSNAME AND A.VWCTID = C.VWCTID
            LEFT JOIN AHMMODCM_DTLPRMPRES D ON B.VINSNAME = D.VINSNAME AND B.VNO = D.VNO
            WHERE A.VINSNAME = :insight_name
        """, {"insight_name": ORACLE_INSIGHT_NAME})
        rows = cursor.fetchall()
        if not rows:
            logger.error(f"[{_SERVICE_NAME}] Oracle tidak mengembalikan data untuk VINSNAME='{ORACLE_INSIGHT_NAME}'.")
            return None
        wct     = rows[0][1]
        technum = rows[0][2]
        params  = {row[0] for row in rows if row[0]}
        api_url = rows[0][3]
        if not api_url:
            logger.error(f"[{_SERVICE_NAME}] VURLAPI kosong di Oracle.")
            return None
        logger.info(f"[{_SERVICE_NAME}] Oracle lookup OK → wct={wct}, technum={technum}, params={params}, api_url={api_url}")
        return wct, technum, params, api_url
    except Exception as e:
        logger.error(f"[{_SERVICE_NAME}] Gagal query Oracle: {e}.")
        return None
    finally:
        conn.close()

# HELPERS
def is_valid_message(val: dict, filter_wct: str, filter_technum: str) -> bool:
    return val.get("vwctid") == filter_wct and val.get("vmachineid") == filter_technum


def is_nan_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() == "nan":
        return True
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return True
    return False

# WRITE TO INFLUXDB
def write_to_influx(result_list: list) -> bool:
    if not FERNET_KEY:
        logger.error(f"[{_SERVICE_NAME}] FERNET_KEY tidak ditemukan. Skip write InfluxDB.")
        return False
    client = createInfluxConnection(fernetKey=FERNET_KEY)
    if client is None:
        logger.error(f"[{_SERVICE_NAME}] Koneksi InfluxDB gagal. Skip write.")
        return False
    try:
        now_str      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        points       = []
        nan_skipped  = 0
        unknown_keys = {}
        for row in result_list:
            try:
                timestamp_ms = int(float(row["t"]))
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"[{_SERVICE_NAME}] Skip row, timestamp tidak valid: {e}")
                continue
            for key, value in row.items():
                if key == "t":
                    continue
                if is_nan_value(value):
                    nan_skipped += 1
                    continue
                parts = key.split("_", 2)
                if len(parts) != 3:
                    unknown_keys[key] = unknown_keys.get(key, 0) + 1
                    continue
                vwctid, vmachineid, remainder = parts
                nhigh_val = nlow_val = nhhigh_val = nllow_val = None
                if "_ _" in remainder:
                    param_part, threshold_part = remainder.split("_ _", 1)
                    vparam = param_part.strip()
                    threshold_clean = threshold_part.rstrip("_ ").strip()
                    threshold_parts = threshold_clean.split("_")
                    if len(threshold_parts) == 2:
                        try: nhigh_val = float(threshold_parts[0])
                        except ValueError: pass
                        try: nlow_val = float(threshold_parts[1])
                        except ValueError: pass
                else:
                    vparam = remainder.strip()
                try:
                    nvalue = float(value) + 0.0
                except (TypeError, ValueError):
                    logger.warning(f"[{_SERVICE_NAME}] Skip key '{key}', value bukan numerik: {value}")
                    continue
                if math.isnan(nvalue) or math.isinf(nvalue):
                    nan_skipped += 1
                    continue
                fields = {"nvalue": nvalue, "dcrea": now_str, "dmodi": now_str, "vcrea": "PREDICTIVE"}
                if nhigh_val  is not None: fields["nhigh"]  = float(nhigh_val)  + 0.0
                if nlow_val   is not None: fields["nlow"]   = float(nlow_val)   + 0.0
                if nhhigh_val is not None: fields["nhhigh"] = float(nhhigh_val) + 0.0
                if nllow_val  is not None: fields["nllow"]  = float(nllow_val)  + 0.0
                points.append({
                    "measurement": INFLUX_MEASUREMENT,
                    "tags": {"vwctid": vwctid, "vmachineid": vmachineid, "vparam": vparam, "type": "-", "vlineid": "-", "vpartid": "-"},
                    "time": timestamp_ms,
                    "fields": fields
                })
        if nan_skipped > 0:
            logger.info(f"[{_SERVICE_NAME}] Total key di-skip karena NaN: {nan_skipped}")
        if unknown_keys:
            logger.warning(f"[{_SERVICE_NAME}] Skip key format tidak dikenali: {unknown_keys}")
        if not points:
            logger.warning(f"[{_SERVICE_NAME}] Tidak ada point valid untuk ditulis ke InfluxDB.")
            return False
        client.write_points(points, time_precision="ms")
        logger.info(f"[{_SERVICE_NAME}] InfluxDB: {len(points)} point berhasil ditulis ke '{INFLUX_MEASUREMENT}'.")
        return True
    except Exception as e:
        logger.error(f"[{_SERVICE_NAME}] Gagal write ke InfluxDB: {e}")
        return False
    finally:
        client.close()

# PIPELINE
def run_pipeline(batch_data: list, filter_params: set, api_url: str):
    global leftover_buffer
    if not batch_data and not leftover_buffer:
        return
    if batch_data:
        filtered = [{col: row[col] for col in USE_COLUMN if col in row} for row in batch_data]
        filtered = [r for r in filtered if r]
        if not filtered:
            logger.warning(f"[{_SERVICE_NAME}] Tidak ada data valid setelah filter kolom.")
            return
        json_str = json.dumps(filtered)
        json_str = renameJsonParam(pathSource=json_str, pathTarget=None, keySource=KEY_SOURCE_RENAME, keyTarget=KEY_TARGET_RENAME)
        if not isinstance(json_str, str):
            logger.error(f"[{_SERVICE_NAME}] Gagal rename parameter. Batch dilewati.")
            return
        json_str = convertTime(pathSource=json_str, pathTarget=None, keySource=["dcrea"], formatTime="epoch_ms")
        if not isinstance(json_str, str):
            logger.error(f"[{_SERVICE_NAME}] Gagal convert time. Batch dilewati.")
            return
        valid_batch = json.loads(json_str)
    else:
        valid_batch = []

    if leftover_buffer:
        logger.info(f"[{_SERVICE_NAME}] Menggabungkan {len(leftover_buffer)} baris leftover...")
        valid_batch = leftover_buffer + valid_batch
        leftover_buffer = []
        logger.info(f"[{_SERVICE_NAME}] Total setelah gabung leftover: {len(valid_batch)} baris.")

    if not valid_batch:
        logger.warning(f"[{_SERVICE_NAME}] Tidak ada data valid setelah proses awal.")
        return

    logger.info(f"[{_SERVICE_NAME}] Menyimpan {len(valid_batch)} baris data mentah Kafka...")
    if not saveAndAppend(json.dumps(valid_batch), base_dir=SAVE_DIR_RAW):
        logger.warning(f"[{_SERVICE_NAME}] Gagal menyimpan data mentah Kafka.")
    else:
        logger.info(f"[{_SERVICE_NAME}] Data mentah Kafka tersimpan di: {SAVE_DIR_RAW}")

    param_counts = Counter(row.get("param") for row in valid_batch)
    logger.info(f"[{_SERVICE_NAME}] Distribusi param sebelum filter: {dict(param_counts)}")

    valid_batch_filtered = [row for row in valid_batch if row.get("param") in filter_params]
    if not valid_batch_filtered:
        logger.warning(f"[{_SERVICE_NAME}] Tidak ada param yang memenuhi syarat (harus dalam {filter_params}).")
        leftover_buffer = valid_batch
        return

    valid_batch = valid_batch_filtered
    logger.info(f"[{_SERVICE_NAME}] Param lolos filter: {dict(Counter(row.get('param') for row in valid_batch))}")

    # SORT BY t ASCENDING
    try:
        valid_batch = sorted(valid_batch, key=lambda x: float(x.get("t", 0)))
        logger.info(f"[{_SERVICE_NAME}] Data di-sort by 't' ascending. {len(valid_batch)} baris.")
    except Exception as e:
        logger.warning(f"[{_SERVICE_NAME}] Gagal sort data by 't': {e}. Lanjut tanpa sort.")

    wct     = valid_batch[0].get("wct", "UNKNOWN")
    technum = valid_batch[0].get("technum", "UNKNOWN")
    logger.info(f"[{_SERVICE_NAME}] Mengirim {len(valid_batch)} baris ke API... [wct={wct}, technum={technum}]")
    logger.info(f"[{_SERVICE_NAME}] API URL: {api_url}")

    result_list = callApi(url=api_url, payload=json.dumps(valid_batch), timeout=RAPIDMINER_TIMEOUT)
    if result_list is None:
        logger.error(f"[{_SERVICE_NAME}] API gagal. Menyimpan batch ke leftover untuk retry batch berikutnya.")
        leftover_buffer = valid_batch
        return
    if len(result_list) == 0:
        logger.warning(f"[{_SERVICE_NAME}] API kembalikan data kosong. Batch dilewati.")
        return
    logger.info(f"[{_SERVICE_NAME}] API selesai. {len(valid_batch)} → {len(result_list)} baris.")
    write_to_influx(result_list)

# KAFKA CONSUMER
def start_stream():
    if not FERNET_KEY:
        logger.error(f"[{_SERVICE_NAME}] FERNET_KEY tidak ditemukan di .env. Stream dihentikan.")
        return
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=KAFKA_SASL_USERNAME,
            sasl_plain_password=KAFKA_SASL_PASSWORD,
            ssl_cafile=KAFKA_SSL_CAFILE,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_interval_ms=3600000,
            max_poll_records=2000,
        )
    except Exception as e:
        logger.error(f"[{_SERVICE_NAME}] Gagal koneksi ke Kafka: {e}")
        return

    filter_cfg = load_filter_from_oracle()
    if filter_cfg is None:
        logger.error(f"[{_SERVICE_NAME}] Oracle lookup awal gagal. Stream dihentikan.")
        consumer.close()
        return
    filter_wct, filter_technum, filter_params, api_url = filter_cfg

    logger.info(f"[{_SERVICE_NAME}] Stream berjalan... Trigger per pergantian shift")
    logger.info(f"[{_SERVICE_NAME}] SERVER_STATUS  : {os.getenv('SERVER_STATUS', 'dev').upper()}")
    logger.info(f"[{_SERVICE_NAME}] Topic          : {KAFKA_TOPIC}")
    logger.info(f"[{_SERVICE_NAME}] Group ID       : {KAFKA_GROUP_ID}")
    logger.info(f"[{_SERVICE_NAME}] API URL        : {api_url}")
    logger.info(f"[{_SERVICE_NAME}] Filter aktif   : vwctid={filter_wct}, vmachineid={filter_technum}")
    logger.info(f"[{_SERVICE_NAME}] Filter param   : {filter_params}")
    logger.info(f"[{_SERVICE_NAME}] Shift triggers : {SHIFT_TRIGGER_TIMES}\n")

    batch_buffer       = []
    last_executed_shift = ""  # flag agar tidak eksekusi double dalam 1 menit

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for tp, msgs in messages.items():
                for msg in msgs:
                    val = msg.value
                    if val is None:
                        continue
                    if isinstance(val, list):
                        batch_buffer.extend([v for v in val if isinstance(v, dict) and is_valid_message(v, filter_wct, filter_technum)])
                    elif isinstance(val, dict):
                        if is_valid_message(val, filter_wct, filter_technum):
                            batch_buffer.append(val)
                    elif isinstance(val, str):
                        try:
                            parsed = json.loads(val)
                            if isinstance(parsed, list):
                                batch_buffer.extend([v for v in parsed if isinstance(v, dict) and is_valid_message(v, filter_wct, filter_technum)])
                            elif isinstance(parsed, dict):
                                if is_valid_message(parsed, filter_wct, filter_technum):
                                    batch_buffer.append(parsed)
                        except json.JSONDecodeError:
                            continue

            # CEK SHIFT TRIGGER
            should_execute, last_executed_shift = is_shift_trigger(last_executed_shift)

            if should_execute:
                shift_label = get_current_shift_label(last_executed_shift)
                now = datetime.now().strftime("%H:%M:%S")

                # REFRESH FILTER & API URL DARI ORACLE
                filter_cfg = load_filter_from_oracle()
                if filter_cfg is None:
                    logger.error(f"[{_SERVICE_NAME}] Oracle lookup gagal saat refresh. Stream dihentikan.")
                    break
                filter_wct, filter_technum, filter_params, api_url = filter_cfg
                logger.info(f"[{_SERVICE_NAME}] Filter diperbarui dari Oracle → wct={filter_wct}, technum={filter_technum}, params={filter_params}")

                if batch_buffer:
                    logger.info(f"[{_SERVICE_NAME}] [{now}] Trigger {shift_label} — Memproses {len(batch_buffer)} baris... (leftover: {len(leftover_buffer)} baris)")
                    run_pipeline(batch_buffer, filter_params, api_url)
                    batch_buffer = []
                else:
                    if leftover_buffer:
                        logger.info(f"[{_SERVICE_NAME}] [{now}] Trigger {shift_label} — Tidak ada data baru, retry {len(leftover_buffer)} baris leftover...")
                        run_pipeline([], filter_params, api_url)
                    else:
                        logger.info(f"[{_SERVICE_NAME}] [{now}] Trigger {shift_label} — Tidak ada data masuk selama shift ini.")

    except KeyboardInterrupt:
        logger.info(f"[{_SERVICE_NAME}] Stream dihentikan manual.")
    except Exception as e:
        logger.error(f"[{_SERVICE_NAME}] Fatal Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_stream()
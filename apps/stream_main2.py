import json
import math
import time
import logging

import os
from datetime import datetime
from collections import Counter
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
    KAFKA_GROUP_ID
)

from apps.common.callApi import callApi
from apps.common.saveAppender import saveAndAppend
from apps.common.createInfluxConnection import createInfluxConnection
from apps.common.createOracleConnection import createOracleConnection
from apps.common.renameJsonParam import renameJsonParam
from apps.common.convertTime import convertTime
from apps.logger import setupLogger

# CONFIG
BATCH_INTERVAL_SECONDS = 20
RAPIDMINER_TIMEOUT     = 60
SAVE_DIR_RAW           = "apps/data/rawKafka1"
SAVE_DIR_RESULT        = "apps/data/resultResponseAPI1"
INFLUX_CONFIG_PATH     = "apps/config/influxConnection.properties"
INFLUX_MEASUREMENT     = "AHMMODCM_TEST"
ORACLE_CONFIG_PATH     = "apps/config/oracleConnection.properties"
ORACLE_INSIGHT_NAME    = "P9PIA0_IMM04_INSIGHT1"
FERNET_KEY             = os.getenv("FERNET_KEY", "")

USE_COLUMN = [
    "t", "dcrea", "vwctid", "vmachineid", "vparam",
    "nvalue", "catg", "nhhigh", "nhigh", "nlow", "nllow"
]

KEY_SOURCE_RENAME = ["vwctid", "vmachineid", "vparam", "catg",     "nhhigh", "nhigh", "nllow", "nlow"]
KEY_TARGET_RENAME = ["wct",    "technum",    "param",  "category", "hhigh",  "high",  "llow",  "low"]

setupLogger(level=logging.INFO, log_dir="apps/logs")
logger = logging.getLogger(__name__)

# LEFTOVER BUFFER
leftover_buffer = []


# LOAD FILTER FROM ORACLE
def load_filter_from_oracle() -> Optional[Tuple[str, str, Set[str], str]]:
    conn = createOracleConnection(
        pathConfig=ORACLE_CONFIG_PATH,
        fernetKey=FERNET_KEY
    )
    if conn is None:
        logger.error("Koneksi Oracle gagal. Stream dihentikan.")
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
            logger.error(f"Oracle tidak mengembalikan data untuk VINSNAME='{ORACLE_INSIGHT_NAME}'. Stream dihentikan.")
            return None

        wct     = rows[0][1]
        technum = rows[0][2]
        params  = {row[0] for row in rows if row[0]}
        api_url = rows[0][3]

        if not api_url:
            logger.error("VURLAPI kosong di Oracle. Stream dihentikan.")
            return None

        logger.info(f"Oracle lookup OK → wct={wct}, technum={technum}, params={params}, api_url={api_url}")
        return wct, technum, params, api_url

    except Exception as e:
        logger.error(f"Gagal query Oracle: {e}. Stream dihentikan.")
        return None
    finally:
        conn.close()


# FILTER MESSAGE
def is_valid_message(val: dict, filter_wct: str, filter_technum: str) -> bool:
    return (
        val.get("vwctid") == filter_wct and
        val.get("vmachineid") == filter_technum
    )


# HELPER: cek apakah value adalah NaN atau tidak valid
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
        logger.error("FERNET_KEY tidak ditemukan di environment variable. Skip write InfluxDB.")
        return False

    client = createInfluxConnection(
        pathConfig=INFLUX_CONFIG_PATH,
        fernetKey=FERNET_KEY
    )
    if client is None:
        logger.error("Koneksi InfluxDB gagal. Skip write.")
        return False

    try:
        now_str     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        points      = []
        nan_skipped = 0

        for row in result_list:
            try:
                timestamp_ms = int(float(row["t"]))
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Skip row, timestamp tidak valid: {e}")
                continue

            for key, value in row.items():
                if key == "t":
                    continue

                # ── CEK NaN SEBELUM KONVERSI ─────────────────────────────────
                if is_nan_value(value):
                    logger.debug(f"Skip key '{key}' karena value NaN/None (pre-convert).")
                    nan_skipped += 1
                    continue
                # ────────────────────────────────────────────────────────────

                parts = key.split("_", 2)
                if len(parts) != 3:
                    logger.warning(f"Skip key format tidak dikenali: '{key}'")
                    continue

                vwctid, vmachineid, remainder = parts

                nhigh_val  = None
                nlow_val   = None
                nhhigh_val = None
                nllow_val  = None

                if "_ _" in remainder:
                    param_part, threshold_part = remainder.split("_ _", 1)
                    vparam = param_part.strip()

                    threshold_clean = threshold_part.rstrip("_ ").strip()
                    threshold_parts = threshold_clean.split("_")

                    if len(threshold_parts) == 2:
                        try:
                            nhigh_val = float(threshold_parts[0])
                        except ValueError:
                            nhigh_val = None
                        try:
                            nlow_val = float(threshold_parts[1])
                        except ValueError:
                            nlow_val = None
                else:
                    vparam = remainder.strip()

                try:
                    nvalue = float(value) + 0.0
                except (TypeError, ValueError):
                    logger.warning(f"Skip key '{key}', value bukan numerik: {value}")
                    continue

                # ── CEK NaN/Inf SETELAH KONVERSI ─────────────────────────────
                if math.isnan(nvalue) or math.isinf(nvalue):
                    logger.debug(f"Skip key '{key}' karena nvalue={nvalue} tidak valid (post-convert).")
                    nan_skipped += 1
                    continue
                # ────────────────────────────────────────────────────────────

                fields = {
                    "nvalue": nvalue,
                    "dcrea":  now_str,
                    "dmodi":  now_str,
                    "vcrea":  "PREDICTIVE",
                }

                if nhigh_val is not None:
                    fields["nhigh"]  = float(nhigh_val) + 0.0
                if nlow_val is not None:
                    fields["nlow"]   = float(nlow_val) + 0.0
                if nhhigh_val is not None:
                    fields["nhhigh"] = float(nhhigh_val) + 0.0
                if nllow_val is not None:
                    fields["nllow"]  = float(nllow_val) + 0.0

                points.append({
                    "measurement": INFLUX_MEASUREMENT,
                    "tags": {
                        "vwctid":     vwctid,
                        "vmachineid": vmachineid,
                        "vparam":     vparam,
                        "type":       "-",
                        "vlineid":    "-",
                        "vpartid":    "-",
                    },
                    "time": timestamp_ms,
                    "fields": fields
                })

        if nan_skipped > 0:
            logger.info(f"Total key di-skip karena NaN/Inf: {nan_skipped}")

        if not points:
            logger.warning("Tidak ada point valid untuk ditulis ke InfluxDB.")
            return False

        client.write_points(points, time_precision="ms")
        logger.info(f"InfluxDB: {len(points)} point berhasil ditulis ke '{INFLUX_MEASUREMENT}'.")
        return True

    except Exception as e:
        logger.error(f"Gagal write ke InfluxDB: {e}")
        return False
    finally:
        client.close()


# PIPELINE
def run_pipeline(batch_data: list, filter_params: set, api_url: str):
    global leftover_buffer

    if not batch_data and not leftover_buffer:
        return

    if batch_data:
        # 1. FILTER KOLOM
        filtered = []
        for row in batch_data:
            new_row = {}
            for col in USE_COLUMN:
                if col in row:
                    new_row[col] = row[col]
            if new_row:
                filtered.append(new_row)

        if not filtered:
            logger.warning("Tidak ada data valid setelah filter kolom.")
            return

        json_str = json.dumps(filtered)

        # 2. RENAME
        json_str = renameJsonParam(
            pathSource=json_str,
            pathTarget=None,
            keySource=KEY_SOURCE_RENAME,
            keyTarget=KEY_TARGET_RENAME
        )
        if not isinstance(json_str, str):
            logger.error("Gagal rename parameter. Batch dilewati.")
            return

        # 3. CONVERT TIME dcrea → EPOCH_MS
        json_str = convertTime(
            pathSource=json_str,
            pathTarget=None,
            keySource=["dcrea"],
            formatTime="epoch_ms"
        )
        if not isinstance(json_str, str):
            logger.error("Gagal convert time. Batch dilewati.")
            return

        valid_batch = json.loads(json_str)
    else:
        valid_batch = []

    # 3.5 GABUNGKAN DENGAN LEFTOVER BATCH SEBELUMNYA
    if leftover_buffer:
        logger.info(f"Menggabungkan {len(leftover_buffer)} baris leftover dari batch sebelumnya...")
        valid_batch = leftover_buffer + valid_batch
        leftover_buffer = []
        logger.info(f"Total setelah gabung leftover: {len(valid_batch)} baris.")

    if not valid_batch:
        logger.warning("Tidak ada data valid setelah proses awal.")
        return

    # 3.6 SAVE RAW DATA KAFKA
    logger.info(f"Menyimpan {len(valid_batch)} baris data mentah Kafka...")
    raw_save_status = saveAndAppend(json.dumps(valid_batch), base_dir=SAVE_DIR_RAW)
    if not raw_save_status:
        logger.warning("Gagal menyimpan data mentah Kafka.")
    else:
        logger.info(f"Data mentah Kafka tersimpan di: {SAVE_DIR_RAW}")

    # 4. FILTER PARAM
    param_counts = Counter(row.get("param") for row in valid_batch)
    logger.info(f"Distribusi param sebelum filter: {dict(param_counts)}")

    valid_batch_filtered = [
        row for row in valid_batch
        if row.get("param") in filter_params
    ]

    if not valid_batch_filtered:
        logger.warning(
            f"Tidak ada param yang memenuhi syarat (harus dalam {filter_params}). "
            f"Menyimpan {len(valid_batch)} baris ke leftover untuk batch berikutnya."
        )
        leftover_buffer = valid_batch
        return

    valid_batch = valid_batch_filtered

    param_lolos = Counter(row.get("param") for row in valid_batch)
    logger.info(f"Param lolos filter: {dict(param_lolos)}")

    # ── SORT BY t ASCENDING SEBELUM KIRIM KE API ────────────────────────────
    try:
        valid_batch = sorted(valid_batch, key=lambda x: float(x.get("t", 0)))
        logger.info(f"Data di-sort by 't' ascending. {len(valid_batch)} baris.")
    except Exception as e:
        logger.warning(f"Gagal sort data by 't': {e}. Lanjut tanpa sort.")
    # ────────────────────────────────────────────────────────────────────────

    wct     = valid_batch[0].get("wct", "UNKNOWN")
    technum = valid_batch[0].get("technum", "UNKNOWN")

    logger.info(f"Mengirim {len(valid_batch)} baris ke API... [wct={wct}, technum={technum}]")
    logger.info(f"API URL: {api_url}")
    logger.info(f"Sample payload ke API (5 baris): {json.dumps(valid_batch[4], indent=2)}")

    # DEBUG: save sample payload ke file untuk test di Postman
    debug_path = "apps/data/debug_payload.json"
    try:
        with open(debug_path, "w") as f:
            json.dump({"data": valid_batch[:10000]}, f, indent=2)
        logger.info(f"Debug payload disimpan ke: {debug_path} ({min(len(valid_batch), 10000)} baris)")
    except Exception as e:
        logger.warning(f"Gagal simpan debug payload: {e}")

    # 5. CALL API
    result_list = callApi(
        url=api_url,
        payload=json.dumps(valid_batch),
        timeout=RAPIDMINER_TIMEOUT
    )

    if result_list is None:
        logger.error("API gagal. Menyimpan batch ke leftover untuk retry batch berikutnya.")
        leftover_buffer = valid_batch
        return

    if len(result_list) == 0:
        logger.warning("API kembalikan data kosong. Batch dilewati.")
        return

    logger.info(f"API selesai. {len(valid_batch)} → {len(result_list)} baris.")

    # 6. SAVE DATA HASIL API
    result_save_status = saveAndAppend(json.dumps(result_list), base_dir=SAVE_DIR_RESULT)
    if not result_save_status:
        logger.warning("saveAndAppend hasil API gagal.")
    else:
        logger.info(f"Data hasil API tersimpan di: {SAVE_DIR_RESULT} ({len(result_list)} baris)")

    # 7. WRITE TO INFLUXDB
    write_to_influx(result_list)


# KAFKA CONSUMER
def start_stream():
    if not FERNET_KEY:
        logger.error("FERNET_KEY tidak ditemukan di .env. Stream dihentikan.")
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
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_interval_ms=1800000,
            max_poll_records=2000,   # dinaikkan dari 500
        )
    except Exception as e:
        logger.error(f"Gagal koneksi ke Kafka: {e}")
        return

    # INITIAL ORACLE LOOKUP
    filter_cfg = load_filter_from_oracle()
    if filter_cfg is None:
        logger.error("Oracle lookup awal gagal. Stream dihentikan.")
        consumer.close()
        return
    filter_wct, filter_technum, filter_params, api_url = filter_cfg

    logger.info(f"Stream berjalan... Batch per {BATCH_INTERVAL_SECONDS} detik")
    logger.info(f"API URL  : {api_url}")
    logger.info(f"Filter aktif: vwctid={filter_wct}, vmachineid={filter_technum}")
    logger.info(f"Filter param: {filter_params}\n")

    batch_buffer = []
    start_time   = time.time()

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)

            for tp, msgs in messages.items():
                for msg in msgs:
                    val = msg.value
                    if val is None:
                        continue
                    if isinstance(val, list):
                        batch_buffer.extend([
                            v for v in val
                            if isinstance(v, dict) and is_valid_message(v, filter_wct, filter_technum)
                        ])
                    elif isinstance(val, dict):
                        if is_valid_message(val, filter_wct, filter_technum):
                            batch_buffer.append(val)
                    elif isinstance(val, str):
                        try:
                            parsed = json.loads(val)
                            if isinstance(parsed, list):
                                batch_buffer.extend([
                                    v for v in parsed
                                    if isinstance(v, dict) and is_valid_message(v, filter_wct, filter_technum)
                                ])
                            else:
                                if is_valid_message(parsed, filter_wct, filter_technum):
                                    batch_buffer.append(parsed)
                        except json.JSONDecodeError:
                            continue

            elapsed = time.time() - start_time

            if elapsed >= BATCH_INTERVAL_SECONDS:
                # REFRESH FILTER & API URL DARI ORACLE SETIAP BATCH
                filter_cfg = load_filter_from_oracle()
                if filter_cfg is None:
                    logger.error("Oracle lookup gagal saat refresh. Stream dihentikan.")
                    break
                filter_wct, filter_technum, filter_params, api_url = filter_cfg
                logger.info(f"Filter diperbarui dari Oracle → wct={filter_wct}, technum={filter_technum}, params={filter_params}, api_url={api_url}")

                now = datetime.now().strftime("%H:%M:%S")
                if batch_buffer:
                    logger.info(f"[{now}] Memproses {len(batch_buffer)} baris... (leftover: {len(leftover_buffer)} baris)")
                    run_pipeline(batch_buffer, filter_params, api_url)
                    batch_buffer = []
                else:
                    if leftover_buffer:
                        logger.info(f"[{now}] Tidak ada data baru, retry {len(leftover_buffer)} baris leftover...")
                        run_pipeline([], filter_params, api_url)
                    else:
                        logger.info(f"[{now}] Tidak ada data dalam {BATCH_INTERVAL_SECONDS} detik terakhir.")

                start_time = time.time()

    except KeyboardInterrupt:
        logger.info("Stream dihentikan manual.")
    except Exception as e:
        logger.error(f"Fatal Error: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    start_stream()
import os
from dotenv import load_dotenv
from apps.common.decryptData import decryptData

load_dotenv()

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
ENCRYPTED_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD_ENCRYPTED")

if not ENCRYPTION_KEY or not ENCRYPTED_PASSWORD:
    raise EnvironmentError("key tidak ditemukan")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE", "")

raw_servers = os.getenv("KAFKA_SERVERS", "")
KAFKA_SERVERS = [server.strip() for server in raw_servers.split(",") if server.strip()]

KAFKA_SASL_PASSWORD = decryptData(key=ENCRYPTION_KEY, encryptText=ENCRYPTED_PASSWORD)
# KAFKA_SASL_PASSWORD = ""
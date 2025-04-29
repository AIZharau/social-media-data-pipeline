import os
from dotenv import load_dotenv
import json
from pathlib import Path

load_dotenv()

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "cache"
LOG_DIR = BASE_DIR / "logs"
STATE_DIR = BASE_DIR / "state"

# Create necessary directories
for dir_path in [DATA_DIR, CACHE_DIR, LOG_DIR, STATE_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# API Configuration
MS_TOKEN = os.getenv("MS_TOKEN")
TIKTOK_VERIFY_FP = os.getenv("TIKTOK_VERIFY_FP")
TIKTOK_SESSIONID = os.getenv("TIKTOK_SESSIONID")

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "tiktok_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tiktok-data")

# Monitoring Configuration
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", 8001))

# ETL Configuration
ETL_SCHEDULE_INTERVAL = int(os.getenv("ETL_SCHEDULE_INTERVAL", 60))  # Minutes
ETL_RETRY_COUNT = int(os.getenv("ETL_RETRY_COUNT", 3))
ETL_RETRY_DELAY = int(os.getenv("ETL_RETRY_DELAY", 5))  # Seconds
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 2))

# Cache Configuration
TOKEN_CACHE_TTL = int(os.getenv("TOKEN_CACHE_TTL", 3600))  # 1 hour
API_RESPONSE_CACHE_TTL = int(os.getenv("API_RESPONSE_CACHE_TTL", 300))  # 5 minutes

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "etl_pipeline.log")

def get_target_accounts():
    target_accounts_str = os.getenv("TARGET_ACCOUNTS")
    target_urls_json = os.getenv("TARGET_ACCOUNT_URLS")
    
    if target_urls_json:
        try:
            return json.loads(target_urls_json)
        except json.JSONDecodeError:
            return get_default_accounts()
    elif target_accounts_str:
        usernames = [u.strip() for u in target_accounts_str.split(',')]
        return {username: None for username in usernames}
    else:
        return get_default_accounts()

def get_default_accounts():
    return {
        "obschestvoznaika_el": "https://www.tiktok.com/@obschestvoznaika_el",
        "himichka_el": "https://www.tiktok.com/@himichka_el",
        "anglichanka_el": "https://www.tiktok.com/@anglichanka_el",
        "fizik_el": "https://www.tiktok.com/@fizik_el",
        "katya_matematichka": "https://www.tiktok.com/@katya_matematichka"
    } 
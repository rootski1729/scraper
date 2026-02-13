# utils.py

import os
import logging
from datetime import datetime
import random
import string
import uuid

from config import DEVICE_MODELS, DEVICE_BRANDS, OKHTTP_USER_AGENTS, COMPATIBLE_COMPONENTS


def generate_device_uid() -> str:
    """Generate a random device UID in Zepto's format."""
    return ''.join(random.choices(string.hexdigits.lower(), k=16))


def generate_session_id() -> str:
    """Generate a random session ID in Zepto's format."""
    return ''.join(random.choices(string.hexdigits.lower(), k=32))


def generate_request_id() -> str:
    """Generate a random request ID in Zepto's format."""
    return ''.join(random.choices(string.hexdigits.lower(), k=32))


def get_random_user_agent() -> str:
    """Pick a random OkHttp user-agent for headers."""
    return random.choice(OKHTTP_USER_AGENTS)


def get_random_device_model() -> str:
    """Pick a random device model for headers."""
    return random.choice(DEVICE_MODELS)


def get_random_device_brand() -> str:
    """Pick a random device brand for headers."""
    return random.choice(DEVICE_BRANDS)


def get_fresh_headers(store_id: str = None) -> dict:
    """
    Generate a fresh set of headers for Zepto API requests.
    """
    device_uid = generate_device_uid()
    session_id = generate_session_id()
    request_id = generate_request_id()
    user_agent = get_random_user_agent()
    device_model = get_random_device_model()
    device_brand = get_random_device_brand()

    headers = {
        "accept": "application/json",
        "access-control-allow-credentials": "true",
        "x-requested-with": "XMLHttpRequest",
        "sessionid": session_id,
        "session_id": session_id,
        "appversion": "24.7.1",
        "app_version": "24.7.1",
        "deviceuid": device_uid,
        "device_uid": device_uid,
        "platform": "android",
        "systemversion": "14",
        "system_version": "14",
        "source": "PLAY_STORE",
        "device_model": device_model,
        "device_brand": device_brand,
        "compatible_components": COMPATIBLE_COMPONENTS,
        "isinternaluser": "false",
        "is_internal_user": "false",
        "tobaccoconsentgiven": "false",
        "tobacco_consent_given": "false",
        "requestid": request_id,
        "request_id": request_id,
        "bundleversion": "v7",
        "bundle_version": "v7",
        "is_new_font": "true",
        "accept-encoding": "gzip",
        "user_gppo": str(random.randint(1000, 5000)),
        "user_is_pass_user": random.choice(["true", "false"]),
        "user_days_since_last_bought": str(random.randint(1, 30)),
        "user_order_number": str(random.randint(1, 50)),
        "user_variant_hash": str(random.randint(10, 99)),
        "connection": "Keep-Alive",
        "user-agent": user_agent,
        "host": "api.zepto.co.in"
    }

    if store_id:
        headers["storeid"] = store_id
        headers["store_id"] = store_id
        headers["lastselectedstoreid"] = store_id
        headers["last_selected_store_id"] = store_id

    return headers


def setup_environment(client_name: str) -> dict:
    """
    Set up directories and logging for a given client.
    """
    today_date = datetime.now().strftime('%Y-%m-%d')
    try:
        # Create base directories
        for d in ["Logs", "Failed", "Outputs", "Raw_Jsons"]:
            os.makedirs(d, exist_ok=True)

        # Client-specific directories
        failed_dir = os.path.join("Failed", "Zepto", client_name)
        output_dir = os.path.join("Outputs", "Zepto", client_name)
        log_dir = os.path.join("Logs", "Zepto", client_name)
        scraped_data_dir = os.path.join("Raw_Jsons", "Zepto", client_name, today_date)

        for d in [failed_dir, output_dir, log_dir, scraped_data_dir]:
            os.makedirs(d, exist_ok=True)

        # Configure logging to file and console
        log_file = os.path.join(log_dir, f"{today_date}.log")
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            force=True
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logging.getLogger().addHandler(console_handler)

        logging.info(f"Environment setup completed. Log file: {log_file}")
        print(f"Log file created at: {log_file}")

        return {
            "today_date": today_date,
            "scraped_data_dir": scraped_data_dir
        }
    except Exception as e:
        print(f"Error setting up environment: {e}")
        raise 
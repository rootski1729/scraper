# api_helpers.py

import requests
import logging
import time
import json
from typing import Tuple, Optional
import geopy
from config import ZYTE_PROXY, ZYTE_CERT


from utils import get_fresh_headers
from config import RETRY_DELAY


def get_lat_long_zepto(pincode: str, max_retries: int = 1) -> Tuple[Optional[float], Optional[float]]:
    """Get latitude and longitude for a pincode via Zepto's autocomplete and details APIs."""
    headers = get_fresh_headers()
    params = {'place_name': pincode}
    logging.info(f"Getting place ID for pincode {pincode}")
    try:
        response = requests.get(
            'https://api.zeptonow.com/api/v1/maps/place/autocomplete/',
            params=params,
            headers=headers,
            timeout=15
        )
        # with open(f"Place_ID/zepto_autocomplete_{pincode}.json", "w") as f:
        #     json.dump(response.json(), f, indent=4)
        place_id = response.json()['predictions'][0]['place_id']
        logging.info(f"Place ID for pincode {pincode}: {place_id}")
    except Exception as e:
        logging.error(f"Error getting place ID for pincode {pincode}: {e}")
        return None, None

    params = {'place_id': place_id}
    try:
        response = requests.get(
            'https://api.zeptonow.com/api/v1/maps/place/details/',
            params=params,
            headers=headers
        )
        lat = response.json()['result']['geometry']['location']['lat']
        lng = response.json()['result']['geometry']['location']['lng']
        logging.info(f"Latitude and Longitude for pincode {pincode}: {lat}, {lng}")
    except Exception as e:
        logging.error(f"Error getting lat/long: {e}")
        return None, None

    return lat, lng




def get_zepto_store_id(lat: float, lng: float,pincode:str, max_retries: int = 1) -> Optional[str]:
    """Get store ID from Zepto API using latitude and longitude."""
    url = "https://api.zepto.co.in/api/v1/config/layout/"
    for attempt in range(max_retries):
        headers = get_fresh_headers()
        params = {
            "latitude": lat,
            "longitude": lng,
            "page_type": "HOME",
            "version": "v2",
            "show_new_eta_banner": "true"
        }
        logging.info(f"Attempt {attempt+1} to get store ID for coordinates: {lat}, {lng}")
        if attempt > 0:
            delay = RETRY_DELAY * (attempt + 1)
            logging.info(f"Waiting {delay}s before retry...")
            time.sleep(delay)
        try:
            '''response = requests.get(url, headers=headers, params=params, proxies=ZYTE_PROXY, verify=ZYTE_CERT, timeout=15)'''
            response = requests.get(url, headers=headers, params=params, verify=False, timeout=15)
        except Exception as e:
            logging.error(f"Request error: {e}")
            if attempt < max_retries - 1:
                continue
            return None
        if response.status_code != 200:
            logging.error(f"Unexpected status code: {response.status_code}")
            if attempt < max_retries - 1:
                continue
            return None
        logging.info(f"Response text for store ID request: {response.text}")
        try:
            out = response.json()
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e}")
            if attempt < max_retries - 1:
                continue
            return None
        store_serviceable_response = out.get('storeServiceableResponse')
        if store_serviceable_response is None:
            logging.error("No 'storeServiceableResponse' in response")
            if attempt < max_retries - 1:
                continue
            return None
        store_id = store_serviceable_response.get('storeId')
        if not store_id:
            logging.error("No store ID in response")
            if attempt < max_retries - 1:
                continue
            return None
        return store_id
    return None 


#New function to get EDT
def get_edt(lat: float, lng: float, store_id: str, max_retries: int = 2) -> Optional[str]:
    """Get Estimated Delivery Time (EDT) from Zepto API using latitude, longitude, and store ID."""
    url = "https://api.zepto.co.in/api/v2/inventory/banner/eta-info"
    for attempt in range(max_retries):
        headers = get_fresh_headers()
        params = {
            "latitude": lat,
            "longitude": lng,
            "store_id": store_id,
            "version": "v2",
            "show_new_eta_banner": "true"
        }
        logging.info(f"Attempt {attempt+1} to get EDT for store ID: {store_id}")
        if attempt > 0:
            delay = RETRY_DELAY * (attempt + 1)
            logging.info(f"Waiting {delay}s before retry...")
            time.sleep(delay)
        try:
            response = requests.get(url, headers=headers, params=params, verify=False, timeout=15)
        except Exception as e:
            logging.error(f"Request error: {e}")
            if attempt < max_retries - 1:
                continue
            return None
        if response.status_code != 200:
            logging.error(f"Unexpected status code: {response.status_code}")
            if attempt < max_retries - 1:
                continue
            return None
        try:
            out = response.json()
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e}")
            if attempt < max_retries - 1:
                continue
            return None
        edt = out.get('secondaryText')
        logging.info(f"EDT for store ID {store_id}: {edt}")
        if not edt:
            logging.error("No EDT in available slot")
            if attempt < max_retries - 1:
                continue
            return None
        return edt
    return None    
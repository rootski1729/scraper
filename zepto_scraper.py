# scraper_zeto.py

import os
import json
import logging
import pandas as pd
import requests
from datetime import datetime
from typing import Optional, Tuple
from bs4 import BeautifulSoup

from utils import (
    generate_session_id,
    generate_device_uid,
    generate_request_id,
    get_random_device_model,
    get_random_device_brand,
    get_random_user_agent
)
from api_helpers import get_lat_long_zepto, get_zepto_store_id, get_edt
from config import ZYTE_PROXY, ZYTE_CERT, RETRY_DELAY

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def get_city_from_pincode(pincode, country="India"):
    """
    Takes a pincode and returns the best matching city/town name for India.
    """
    geolocator = Nominatim(user_agent="pincode_to_city_app_v1")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    try:
        location = geocode(
            {"postalcode": pincode, "country": country},
            exactly_one=True,
            addressdetails=True
        )

        if not location:
            return "Location not found"

        address = location.raw.get("address", {})

        # Indian address priority
        city = (
            address.get("city")
            or address.get("town")
            or address.get("suburb")
            or address.get("county")
            or address.get("state_district")
            or address.get("district")
            or address.get("state")
        )

        return city if city else "City not found in address data"

    except Exception as e:
        return f"Error: {e}"


def extract_sku_from_url(url: str) -> str:
    """Extract SKU ID from Zepto PDP URL."""
    return url.split('/')[-1].split('?')[0]


def get_store_id_for_pincode(pincode: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """Get store ID, latitude, and longitude for a pincode."""
    # First try to get lat/lng
    lat, lng = get_lat_long_zepto(pincode)
    if not lat or not lng:
        logging.error(f"Could not get coordinates for pincode {pincode}")
        return None, None, None

    # Then get store ID
    store_id = get_zepto_store_id(lat, lng, pincode)
    if not store_id:
        logging.error(f"Could not get store ID for pincode {pincode}")
        return None, None, None

    return store_id, lat, lng


def scrape_product_title(pdp_url: str) -> str:
    """Scrape product title from the Zepto PDP page HTML."""
    try:
        logging.info(f"Scraping product title from URL: {pdp_url}")

        session = requests.Session()
        requests.packages.urllib3.disable_warnings()

        if ZYTE_PROXY:
            response = session.get(pdp_url, timeout=20, verify=False, proxies=ZYTE_PROXY)
        else:
            response = session.get(pdp_url, timeout=15, verify=False)

        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the title element based on the provided HTML structure
        title_element = soup.find('span', class_='text-sm font-semibold leading-[14px] text-[#101418]')
        if title_element:
            title = title_element.get_text(strip=True)
            logging.info(f"Successfully scraped product title: {title}")
            return title

        logging.warning(f"Could not find product title in HTML for URL: {pdp_url}")
        return ''

    except Exception as e:
        logging.error(f"Error scraping product title from {pdp_url}: {e}")
        return ''


def scrape_product(pdp_url: str, pincode: str, platform: str = "zepto", f_brand: str = "origami") -> Optional[pd.Series]:
    """Scrape product data from Zepto API using PDP URL and pincode."""

    # Get today's date for source_date
    source_date = datetime.now().strftime('%d-%m-%Y')

    # Get city from pincode
    city = get_city_from_pincode(pincode)
    logging.info(f"City for pincode {pincode}: {city}")

    # Extract SKU ID from URL
    sku_id = extract_sku_from_url(pdp_url)
    logging.info(f"Extracted SKU ID: {sku_id} from URL: {pdp_url}")

    # Scrape product title from HTML
    scraped_title = scrape_product_title(pdp_url)

    # Get store information
    store_id, lat, lng = get_store_id_for_pincode(pincode)
    if not store_id:
        logging.error("Failed to get store information")
        return None

    logging.info(f"Store ID: {store_id}, Coordinates: {lat}, {lng}")

    # Get EDT and extract only the number
    edt_raw = get_edt(lat, lng, store_id)
    import re
    numbers = re.findall(r'\d+', str(edt_raw))
    edt = numbers[0] if numbers else ''
    logging.info(f"EDT: {edt} (raw: {edt_raw})")

    # Prepare API request
    url = "https://api.zepto.co.in/api/v1/inventory/catalogue/product-detail/"
    session_id = generate_session_id()
    device_uid = generate_device_uid()
    request_id = generate_request_id()

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
        "device_model": get_random_device_model(),
        "device_brand": get_random_device_brand(),
        "user-agent": get_random_user_agent(),
        "host": "api.zepto.co.in",
        "storeid": store_id,
        "store_id": store_id,
        "lastselectedstoreid": store_id,
        "last_selected_store_id": store_id
    }

    query = {
        "product_variant_id": sku_id,
        "store_id": store_id,
        "is_zepto_three_enabled": "true"
    }

    try:
        session = requests.Session()
        requests.packages.urllib3.disable_warnings()
        import time
        time.sleep(RETRY_DELAY)

        if ZYTE_PROXY:
            response = session.get(url, params=query, headers=headers, timeout=20, verify=False, proxies=ZYTE_PROXY)
        else:
            response = session.get(url, params=query, headers=headers, timeout=15, verify=False)

        if response.status_code == 404:
            logging.info("Product not found (404)")
            return pd.Series({
                'source_date': source_date,
                'platform': platform,
                'f_brand': f_brand,
                'city': city,
                'sku': sku_id,
                'pincode': pincode,
                'title': 'Item Not Found',
                'mrp': '',
                'live_price': '',
                'is_available': 'Item Not Found',
                'edt': edt
            })

        if response.status_code != 200:
            logging.error(f"Request failed with status code {response.status_code}")
            return None

        data = response.json()

        # Extract basic info
        # Use scraped title if available, otherwise fall back to API response
        if scraped_title:
            product_name = scraped_title
        else:
            try:
                product_name = data['product']['name']
            except (KeyError, TypeError):
                product_name = 'Unknown'

        try:
            brand = data['product']['brand']
        except (KeyError, TypeError):
            brand = ''

        try:
            product_id = data['product']['id']
            zepto_url = f"https://www.zeptonow.com/product/{product_id}"
        except (KeyError, TypeError):
            zepto_url = pdp_url

        # Extract pricing and availability
        try:
            mrp = data['product']['storeProducts'][0]['productVariant']['mrp'] / 100
        except (KeyError, IndexError, TypeError):
            mrp = ''

        try:
            price = data['product']['storeProducts'][0]['discountedSellingPrice'] / 100
        except (KeyError, IndexError, TypeError):
            try:
                price = data['product']['storeProducts'][0]['sellingPrice'] / 100
            except (KeyError, IndexError, TypeError):
                price = ''

        try:
            availability = 'Yes' if not data['product']['storeProducts'][0]['outOfStock'] else 'No'
        except (KeyError, IndexError, TypeError):
            availability = 'Unknown'

        try:
            quantity = data['product']['storeProducts'][0]['productVariant']['formattedPacksize']
        except (KeyError, IndexError, TypeError):
            quantity = ''

        return pd.Series({
            'source_date': source_date,
            'platform': platform,
            'f_brand': f_brand,
            'city': city,
            'sku': sku_id,
            'pincode': pincode,
            'title': product_name,
            'mrp': mrp,
            'live_price': price,
            'is_available': availability,
            'edt': edt
        })

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")
        return None
    except Exception as e:
        logging.error(f"Error processing product: {e}")
        return None


def main():
    # Hardcoded values - change these as needed
    PDP_URL = "https://www.zepto.com/pn/lizol-rose-fresh-shakti-disinfectant-floor-cleaner/pvid/9a7cdd91-3d2f-4b30-9219-9c66fd6950d8"  # Example URL
    PINCODE = "500001"  # Example pincode

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Starting Zepto product scraper")
    logging.info(f"PDP URL: {PDP_URL}")
    logging.info(f"Pincode: {PINCODE}")

    # Scrape the product
    result = scrape_product(PDP_URL, PINCODE)

    if result is None:
        logging.error("Failed to scrape product")
        return

    # Create DataFrame and save to CSV
    df = pd.DataFrame([result])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"zepto_product_{timestamp}.csv"

    df.to_csv(csv_filename, index=False)
    logging.info(f"Product data saved to {csv_filename}")
    print(f"Product data saved to {csv_filename}")

    # Print the result
    print("\nScraped Product Data:")
    print("=" * 50)
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()

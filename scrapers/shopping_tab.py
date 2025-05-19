import os
import time
import datetime
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
BASE_URL = "https://api.valueserp.com/search"

# Default geolocation settings
AUS_PARAMS = {
    "search_type": "shopping",
    "location": "Australia",
    "google_domain": "google.com",
    "gl": "au",
    "hl": "en"
}
US_PARAMS = {
    "search_type": "shopping",
    "location": "Texas,United States",
    "google_domain": "google.com",
    "gl": "us",
    "hl": "en"
}


def fetch_aus_results(keyword: str) -> list[dict]:
    """
    Fetches shopping results for a keyword from the Australian SERP.
    """
    params = {**AUS_PARAMS, "api_key": API_KEY, "q": keyword}
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("shopping_results", [])


def fetch_us_filters(keyword: str) -> str:
    """
    Fetches filter options for a keyword from the US SERP and concatenates them.
    """
    params = {**US_PARAMS, "api_key": API_KEY, "q": keyword}
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    filters = data.get("filters", [])

    entries = []
    for f in filters:
        name = f.get("name")
        for val in f.get("values", []):
            entries.append(f"{name} - {val.get('name')}")
    return ", ".join(entries)


def scrape_shopping_tab_for_keywords(keywords: list[str]) -> list[dict]:
    """
    For each keyword, fetch AUS shopping results and US filters,
    then merge into a flat record per product listing.

    Returns a list of dicts:
      - keyword, date, filters, plus all other fields from the AUS result (excluding 'image').
    """
    all_records = []
    timestamp = datetime.datetime.utcnow().isoformat()

    for kw in keywords:
        try:
            aus_results = fetch_aus_results(kw)
            filters = fetch_us_filters(kw)

            for item in aus_results:
                record = {"keyword": kw, "date": timestamp, "filters": filters}
                # Copy all fields except 'image'
                for k, v in item.items():
                    if k.lower() == "image":
                        continue
                    record[k] = v
                all_records.append(record)
        except requests.HTTPError as err:
            print(f"HTTP error for '{kw}': {err}")
        except Exception as e:
            print(f"Error processing '{kw}': {e}")

        # Rate-limit between keywords
        time.sleep(1)

    return all_records




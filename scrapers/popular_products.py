import os
import time
import datetime
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
BASE_URL = "https://api.valueserp.com/search"


def fetch_popular_products(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Fetches the top N "popular products" entries from the ValueSERP popular_products payload.
    """
    params = {
        "api_key": API_KEY,
        "q": keyword,
        "location": location,
        "gl": "au",
        "hl": "en",
        "google_domain": "google.com.au",
        "include_ai_overview": "false",
        "ads_optimized": "false",
        "engine": "google"
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    popular = data.get("popular_products", [])[:top_n]
    scrape_date = datetime.datetime.utcnow().isoformat()
    records = []

    for item in popular:
        # Handle price and raw price formats
        price = None
        price_raw = None
        if item.get("price") is not None:
            try:
                price = float(item["price"])
                price_raw = str(item["price"])
            except (TypeError, ValueError):
                price_raw = str(item.get("price"))
        elif isinstance(item.get("regular_price"), dict):
            val = item["regular_price"].get("value")
            if val is not None:
                price = float(val)
                symbol = item["regular_price"].get("symbol", "")
                price_raw = f"{symbol}{val}"
        else:
            price_raw = item.get("product_status")

        records.append({
            "scrape_date": scrape_date,
            "keyword": keyword,
            "position": item.get("position"),
            "product_id": None,
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "rating": item.get("rating"),
            "reviews": item.get("reviews"),
            "price": price,
            "price_raw": price_raw,
            "merchant": item.get("merchant", ""),
            "is_carousel": False,
            "carousel_position": None,
            "has_product_page": False
        })

    time.sleep(1)
    return records


def scrape_for_keywords(keywords: list[str], top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Runs fetch_popular_products() for each keyword and returns a flat list of records.
    """
    all_results = []
    for kw in keywords:
        try:
            results = fetch_popular_products(kw, top_n=top_n, location=location)
            all_results.extend(results)
        except requests.HTTPError as e:
            print(f"HTTP error for '{kw}': {e}")
        except Exception as e:
            print(f"Error fetching popular products for '{kw}': {e}")
    return all_results


if __name__ == "__main__":
    sample_kw = ["mens running shoes"]
    for record in scrape_for_keywords(sample_kw):
        print(record)

import os
import time
import datetime
import requests
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
BASE_URL = "https://api.valueserp.com/search"


def extract_product_id_from_link(link: str) -> str:
    """
    Extracts product ID from Google Shopping product links.
    Example: https://www.google.com.au/shopping/product/16052668069645325775
    Returns: 16052668069645325775
    """
    if not link:
        return None
    
    # Pattern to match Google Shopping product URLs
    pattern = r'/shopping/product/(\d+)'
    match = re.search(pattern, link)
    
    if match:
        return match.group(1)
    
    # Fallback: try to get the last part if it's numeric
    parts = link.rstrip('/').split('/')
    if parts and parts[-1].isdigit():
        return parts[-1]
    
    return None


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

    # Debug: Print the structure of one item to see available fields
    popular = data.get("popular_products", [])
    if popular:
        print("DEBUG - Available fields in first popular product:")
        print(list(popular[0].keys()))
        print("DEBUG - First item sample:")
        print(popular[0])
        print("-" * 50)

    popular = popular[:top_n]
    scrape_date = datetime.datetime.utcnow().isoformat()
    records = []

    for item in popular:
        # Try different possible field names for the link
        link = (item.get("link") or 
                item.get("product_link") or 
                item.get("url") or 
                item.get("shopping_link") or 
                "")
        
        # Extract product ID from the link
        product_id = extract_product_id_from_link(link)
        
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
            "product_id": product_id,  # Now extracted from link
            "title": item.get("title", ""),
            "link": link,  # Now tries multiple field names
            "rating": item.get("rating"),
            "reviews": item.get("reviews"),
            "price": price,
            "price_raw": price_raw,
            "merchant": item.get("merchant", ""),
            "is_carousel": False,
            "carousel_position": None,
            "has_product_page": bool(link)  # True if we have a link
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
    # Test with a sample keyword to see the debug output
    sample_kw = ["mens running shoes"]
    for record in scrape_for_keywords(sample_kw):
        print(f"Title: {record['title']}")
        print(f"Link: {record['link']}")
        print(f"Product ID: {record['product_id']}")
        print(f"Price: {record['price']}")
        print("-" * 30)
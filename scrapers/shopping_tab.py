# ── scrapers/shopping_tab.py ──

import os
import time
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from typing import Any  # ← Added so Any is defined

# ── Load environment variables ─────────────────────────────────────────────────────────────────────
load_dotenv()

API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
BASE_URL = "https://api.valueserp.com/search"

# How many threads to spin up (default: 3)
PARALLEL_REQUESTS = int(os.getenv("PARALLEL_REQUESTS", "3"))

# Delay (in seconds) between consecutive requests, to avoid hammering the API
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "5"))

# Timeout for API requests (default: 45s, increased from 30s)
REQUEST_TIMEOUT = int(os.getenv("VALUESERP_TIMEOUT", "45"))


def create_retry_session():
    """Create a requests session with retry logic for handling timeouts and server errors."""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,                    # Total number of retries
        backoff_factor=2,           # Wait 2s, then 4s, then 8s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
        allowed_methods=["GET"]     # Only retry GET requests
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def extract_product_id_from_link(link: str) -> str:
    """
    Extracts product ID from Google Shopping product links.
    Example: https://www.google.com.au/shopping/product/16052668069645325775
    Returns: "16052668069645325775" or None if not found.
    """
    if not link:
        return None

    pattern = r"/shopping/product/(\d+)"
    match = re.search(pattern, link)
    if match:
        return match.group(1)

    parts = link.rstrip("/").split("/")
    if parts and parts[-1].isdigit():
        return parts[-1]

    return None


def fetch_aus_results_with_filters(keyword: str) -> tuple[list[dict], str]:
    """
    Fetches Australian shopping results AND filters for a keyword from the ValueSERP shopping engine.
    Returns both shopping results and concatenated filters string.
    Now includes retry logic and better error handling.
    """
    params = {
        "api_key": API_KEY,
        "search_type": "shopping",
        "location": "Australia",
        "google_domain": "google.com",
        "gl": "au",
        "hl": "en",
        "q": keyword,
    }
    
    session = create_retry_session()
    
    try:
        response = session.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        shopping_results = data.get("shopping_results", [])
        
        # Extract filters from the response - simple and robust
        filters = data.get("filters", [])
        filter_entries = []
        
        for filter_group in filters:
            filter_name = filter_group.get("name", "")
            filter_values = filter_group.get("values", [])
            
            for value_item in filter_values:
                if isinstance(value_item, dict):
                    value_name = value_item.get("name", "")
                    if filter_name and value_name:
                        filter_entries.append(f"{filter_name} - {value_name}")
                elif isinstance(value_item, str):
                    if filter_name and value_item:
                        filter_entries.append(f"{filter_name} - {value_item}")
        
        filters_str = ", ".join(filter_entries)
        
        return shopping_results, filters_str
        
    except requests.exceptions.Timeout:
        return [], ""
    except requests.exceptions.HTTPError as e:
        return [], ""
    except requests.exceptions.RequestException as e:
        return [], ""
    except Exception as e:
        return [], ""


def process_keyword(keyword: str) -> list[dict]:
    """
    Wrapper around fetch_aus_results_with_filters() with clean, concise logging.
    """
    start_time = time.time()
    try:
        # Single API call to get both Australian shopping results AND filters
        aus_results, filters_str = fetch_aus_results_with_filters(keyword)

        # Count filter entries for logging
        filter_count = len(filters_str.split(", ")) if filters_str else 0

        # Build a record for each item in aus_results
        timestamp = datetime.datetime.utcnow().isoformat()
        results = []

        for item in aus_results:
            link = item.get("link", "")
            product_id = item.get("id")
            if not product_id and link:
                product_id = extract_product_id_from_link(link)

            # Build each record, ensuring that NOT NULL columns never remain None
            record = {
                "scrape_date": timestamp,
                "keyword": keyword,
                "position": item.get("position"),
                "product_id": product_id or "",  # Make sure it's never None if schema requires NOT NULL
                "title": item.get("title", ""),
                "link": link,
                "rating": float(item["rating"]) if (item.get("rating") is not None and _is_number(item["rating"])) else None,
                "reviews": int(item["reviews"]) if (item.get("reviews") is not None and _is_number(item["reviews"])) else None,
                "price": float(item["price"]) if (item.get("price") is not None and _is_number(item["price"])) else None,
                "price_raw": str(item.get("price")) if item.get("price") is not None else "",
                "merchant": item.get("merchant", ""),
                # Use filters from the same Australian API response
                "filters_raw": filters_str,
                # Shopping items aren't in a carousel by default
                "is_carousel": False,
                "carousel_position": None,
                "has_product_page": bool(link),
            }
            results.append(record)

        elapsed = time.time() - start_time
        
        if results:
            print(f"✅ {keyword:<20} → {len(results)} products, {filter_count} filters ({elapsed:.1f}s)")
        else:
            print(f"❌ {keyword:<20} → 0 products, {filter_count} filters ({elapsed:.1f}s)")
        
        return results

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"⚠️ {keyword:<20} → ERROR: {str(e)[:30]}... ({elapsed:.1f}s)")
        return []


def _is_number(val: Any) -> bool:
    """
    Return True if val (string or number) can be safely cast to float, else False.
    """
    try:
        float(val)
        return True
    except Exception:
        return False


def scrape_shopping_tab_for_keywords(
    keywords: list[str],
    parallel: bool = True
) -> list[dict]:
    """
    For each keyword, fetch AUS shopping results + US filters, returning a flat list of records.

    Args:
      keywords: list of keyword strings
      parallel: True to use ThreadPoolExecutor (default); False for sequential

    Returns:
      Combined list of all product dicts from every keyword.
    """
    all_records: list[dict] = []
    total_start = time.time()

    print(f"\n🛒 Shopping Tab Scraper (ValueSERP)")
    print(f"Keywords: {len(keywords)} | Parallel: {'Yes' if parallel else 'No'} | Timeout: {REQUEST_TIMEOUT}s")
    print("-" * 60)

    if parallel and len(keywords) > 1:
        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            future_to_kw = {
                executor.submit(process_keyword, kw): kw
                for kw in keywords
            }
            for future in as_completed(future_to_kw):
                kw = future_to_kw[future]
                try:
                    recs = future.result()
                    all_records.extend(recs)
                except Exception as e:
                    print(f"✗ Failed to process '{kw}': {e}")
    else:
        for kw in keywords:
            recs = process_keyword(kw)
            all_records.extend(recs)

    total_elapsed = time.time() - total_start
    print("-" * 60)
    print(f"🛒 Completed in {total_elapsed:.1f}s | Products: {len(all_records):,}")
    
    if keywords:
        keywords_with_results = len(set(r['keyword'] for r in all_records if r))
        success_rate = (keywords_with_results / len(keywords)) * 100
        keywords_with_results = len(set(r['keyword'] for r in all_records if r))
        print(f"   Success rate: {success_rate:.1f}% ({keywords_with_results}/{len(keywords)} keywords)\n")
    else:
        print()

    return all_records


# If you run this file directly, you'll see a quick smoke test:
if __name__ == "__main__":
    sample_keywords = ["mens running shoes", "wool knit jumper", "laptop bag"]

    print("\n--- SHOPPING TAB SCRAPER TEST ---")
    results = scrape_shopping_tab_for_keywords(sample_keywords, parallel=False)

    print(f"Test completed: {len(results)} total products found")
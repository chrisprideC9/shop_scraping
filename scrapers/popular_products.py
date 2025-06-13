# ── scrapers/popular_products.py ──

import os
import time
import datetime
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from typing import Any  # Needed for type hints in _is_number

# ── Load environment variables ─────────────────────────────────────────────────────────────────────
load_dotenv()

API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
BASE_URL = "https://api.valueserp.com/search"

# How many threads to spin up (default: 3)
PARALLEL_REQUESTS = int(os.getenv("PARALLEL_REQUESTS", "3"))

# Delay (in seconds) between consecutive requests, to avoid hammering the API
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "3"))


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


def _is_number(val: Any) -> bool:
    """
    Return True if val (string or number) can be safely cast to float, else False.
    Used for deciding whether to convert to a float or drop to None.
    """
    try:
        float(val)
        return True
    except Exception:
        return False


def fetch_popular_products(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Fetches up to top_n “popular_products” entries from the ValueSERP payload
    for a single keyword. Returns a list of dicts, each containing fields like:
      - scrape_date, keyword, position, product_id, title, link, rating, reviews,
        price, price_raw, merchant, is_carousel, carousel_position, has_product_page, filters_raw
    Ensures no NOT NULL columns remain None (e.g. product_id -> "") and
    that numeric fields do not remain NaN.
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
        "engine": "google",  # ensure you get the “popular_products” block
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.HTTPError as err:
        # Print the HTTP error plus the API’s own error message
        print(f"✗ HTTP error for '{keyword}': {err}")
        if err.response is not None:
            print("Response body:", err.response.text)
        return []
    data = response.json()

    popular = data.get("popular_products", [])

    # Only show the debug fields for the *first* keyword processed, if any
    if popular:
        print(f"DEBUG - Available fields in first popular product for '{keyword}':")
        print(list(popular[0].keys()))
        print("DEBUG - First item sample:")
        print(popular[0])
        print("-" * 50)

    # Truncate to top_n
    popular = popular[:top_n]
    scrape_date = datetime.datetime.utcnow().isoformat()
    records: list[dict] = []

    for item in popular:
        # There are multiple possible keys for the “link” field, so we try each
        link = (
            item.get("link")
            or item.get("product_link")
            or item.get("url")
            or item.get("shopping_link")
            or ""
        )

        # Extract numeric product_id if possible; ensure NOT NULL => "" 
        pid = extract_product_id_from_link(link) or ""
        # If ValueSERP provides "id", use that if extract didn’t find anything
        if not pid and item.get("id"):
            pid = str(item["id"])

        # Normalize rating: force to float if possible, else None
        rat = None
        if item.get("rating") is not None and _is_number(item["rating"]):
            rat = float(item["rating"])

        # Normalize reviews: force to int if possible, else 0
        rev = 0
        if item.get("reviews") is not None and _is_number(item["reviews"]):
            # Sometimes reviews come as e.g. "380" (string); cast to int safely
            rev = int(float(item["reviews"]))
        # If they send no reviews at all, rev=0 is a safe default

        # Normalize price/price_raw: drop pandas/NumPy NaN => None + "" 
        price_val = None
        price_raw_val = ""
        if item.get("price") is not None:
            if _is_number(item["price"]):
                price_val = float(item["price"])
                price_raw_val = str(item["price"])
            else:
                # e.g. they returned "N/A" or something; treat it as raw string
                price_raw_val = str(item["price"])
        elif isinstance(item.get("regular_price"), dict):
            val = item["regular_price"].get("value")
            if val is not None and _is_number(val):
                price_val = float(val)
                symbol = item["regular_price"].get("symbol", "")
                price_raw_val = f"{symbol}{val}"
            else:
                # if regular_price exists but has no numeric "value"
                price_raw_val = item["regular_price"].get("symbol", "")

        # Build the record dict exactly like your “upload_scrape_data” expects
        records.append({
            "scrape_date": scrape_date,
            "keyword": keyword,
            "position": item.get("position") or 0,  # position INT; if missing, default to 0
            "product_id": pid,                       # guaranteed not None
            "title": item.get("title", "") or "",     # guaranteed not None
            "link": link,                             # text column accepts empty string
            "rating": rat,                            # either float or None
            "reviews": rev,                           # integer
            "price": price_val,                       # either float or None
            "price_raw": price_raw_val,               # guaranteed a string (maybe "")
            "merchant": item.get("merchant", "") or "",  # guaranteed not None
            # Popular-products never has a “carousel” concept—hard‐code these
            "is_carousel": False,
            "carousel_position": None,
            "has_product_page": bool(link),
            # No filters on “popular_products” → just pass empty string
            "filters_raw": ""
        })

    # Rate limit before returning
    time.sleep(RATE_LIMIT_DELAY)
    return records


def process_keyword(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Wrapper around fetch_popular_products() that logs timing and count,
    identical to the shopping‐tab scraper’s process_keyword.
    """
    start_time = time.time()
    try:
        print(f"Processing: {keyword}")
        results = fetch_popular_products(keyword, top_n=top_n, location=location)
        elapsed = time.time() - start_time
        print(f"✓ Completed {keyword} in {elapsed:.2f}s - Found {len(results)} products")
        return results

    except requests.HTTPError as err:
        print(f"✗ HTTP error for '{keyword}': {err}")
        return []
    except Exception as e:
        print(f"✗ Error fetching popular products for '{keyword}': {e}")
        return []
    finally:
        # Rate‐limit has already been applied inside fetch_popular_products
        pass


def scrape_for_keywords(
    keywords: list[str],
    top_n: int = 10,
    location: str = "Australia",
    parallel: bool = True
) -> list[dict]:
    """
    Runs fetch_popular_products() for each keyword, either in parallel threads
    or sequentially, and returns a flat list of all product records.

    Args:
      keywords:   list of keyword strings
      top_n:      how many “popular” items per keyword
      location:   “Australia” (or any other supported location string)
      parallel:   True to use ThreadPoolExecutor, False for serial

    Returns:
      A combined list of all dicts from every keyword.
    """
    all_records: list[dict] = []
    total_start = time.time()

    print(f"\nStarting popular-products scraper for {len(keywords)} keywords")
    print(f"Parallel processing: {'Yes' if parallel else 'No'}")
    print(f"Rate limit delay: {RATE_LIMIT_DELAY}s between requests")
    print("=" * 60)

    if parallel and len(keywords) > 1:
        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            future_to_kw = {
                executor.submit(process_keyword, kw, top_n, location): kw
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
            recs = process_keyword(kw, top_n, location)
            all_records.extend(recs)

    total_elapsed = time.time() - total_start
    print("=" * 60)
    print(f"Completed! Total time: {total_elapsed:.2f}s")
    print(f"Total products found: {len(all_records)}")
    if keywords:
        avg_time = total_elapsed / len(keywords)
        print(f"Average time per keyword: {avg_time:.2f}s\n")
    else:
        print()

    return all_records


# If you run this file directly, you’ll see a quick smoke test + debug output:
if __name__ == "__main__":
    sample_keywords = ["mens running shoes", "wool knit jumper", "laptop bag"]

    print("\n--- SEQUENTIAL TEST ---")
    results_seq = scrape_for_keywords(sample_keywords, parallel=False)

    print("\n--- PARALLEL TEST ---")
    results_par = scrape_for_keywords(sample_keywords, parallel=True)

    print(f"\nSequential found: {len(results_seq)} products")
    print(f"Parallel found:   {len(results_par)} products")

# ── scrapers/shopping_tab.py ──

import os
import time
import datetime
import requests
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


def fetch_aus_results(keyword: str) -> list[dict]:
    """
    Fetches Australian shopping results for a keyword from the ValueSERP shopping engine.
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
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    shopping_results = data.get("shopping_results", [])

    # Debug: print how many products returned for this keyword (only once per keyword)
    if shopping_results:
        print(f"\nDEBUG - Shopping results for '{keyword}':")
        print(f"Found {len(shopping_results)} products")
        print("-" * 50)

    return shopping_results


def fetch_us_filters(keyword: str) -> str:
    """
    Fetches US 'filters' for a keyword (if SKIP_US_FILTERS is False) and concatenates them.
    """
    if os.getenv("SKIP_US_FILTERS", "false").lower() == "true":
        return ""

    params = {
        "api_key": API_KEY,
        "search_type": "shopping",
        "location": "Texas,United States",
        "google_domain": "google.com",
        "gl": "us",
        "hl": "en",
        "q": keyword,
    }
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    filters = data.get("filters", [])
    entries = []
    for f in filters:
        name = f.get("name")
        for val in f.get("values", []):
            entries.append(f"{name} - {val.get('name')}")
    return ", ".join(entries)


def process_keyword(keyword: str) -> list[dict]:
    """
    Wrapper around fetch_aus_results() and fetch_us_filters() that logs timing and count,
    mirroring the popular-products scraper’s pattern.
    """
    start_time = time.time()
    try:
        print(f"Processing: {keyword}")

        # 1) Fetch Australian shopping results
        aus_results = fetch_aus_results(keyword)

        # 2) Fetch US filters (unless SKIP_US_FILTERS=true)
        filters_str = fetch_us_filters(keyword)

        # 3) Build a record for each item in aus_results
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
                # We put all filter entries into a single CSV string here
                "filters_raw": filters_str,
                # Shopping items aren’t in a carousel by default
                "is_carousel": False,
                "carousel_position": None,
                "has_product_page": bool(link),
            }
            results.append(record)

        elapsed = time.time() - start_time
        print(f"✓ Completed {keyword} in {elapsed:.2f}s - Found {len(results)} products")
        return results

    except requests.HTTPError as err:
        print(f"✗ HTTP error for '{keyword}': {err}")
        return []
    except Exception as e:
        print(f"✗ Error processing '{keyword}': {e}")
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

    print(f"\nStarting shopping-tab scraper for {len(keywords)} keywords")
    print(f"Parallel processing: {'Yes' if parallel else 'No'}")
    print(f"SKIP_US_FILTERS: {os.getenv('SKIP_US_FILTERS', 'false')}")
    print(f"Rate limit delay: {RATE_LIMIT_DELAY}s between requests")
    print("=" * 60)

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
    results_seq = scrape_shopping_tab_for_keywords(sample_keywords, parallel=False)

    print("\n--- PARALLEL TEST ---")
    results_par = scrape_shopping_tab_for_keywords(sample_keywords, parallel=True)

    print(f"\nSequential found: {len(results_seq)} products")
    print(f"Parallel found:   {len(results_par)} products")

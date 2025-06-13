# ── scrapers/popular_products.py ──

import os
import time
import datetime
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from typing import Any, List, Dict  # Needed for type hints

# ── Load environment variables ─────────────────────────────────────────────────────────────────────
load_dotenv()

# Primary API (ScrapingDog)
SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY", "684a2dcee2392898dd06564a")
SCRAPINGDOG_BASE_URL = "https://api.scrapingdog.com/google"

# Fallback API (ValueSERP)
VALUESERP_API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
VALUESERP_BASE_URL = "https://api.valueserp.com/search"

# How many threads to spin up (default: 3)
PARALLEL_REQUESTS = int(os.getenv("PARALLEL_REQUESTS", "3"))

# Delay (in seconds) between consecutive requests, to avoid hammering the API
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "5"))

# Minimum products threshold - if we get less than this, try fallback
FALLBACK_THRESHOLD = int(os.getenv("POPULAR_PRODUCTS_FALLBACK_THRESHOLD", "5"))


def extract_product_id_from_link(link: str) -> str:
    """
    Extracts product ID from Google Shopping product links.
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


def parse_reviews_count(reviews_str: str) -> int:
    """Parse review count strings like "7.7K", "123", "2.5M" into integers."""
    if not reviews_str or not isinstance(reviews_str, str):
        return 0
    
    reviews_str = reviews_str.strip().replace('(', '').replace(')', '')
    
    if reviews_str.upper().endswith('K'):
        try:
            num = float(reviews_str[:-1])
            return int(num * 1000)
        except ValueError:
            return 0
    elif reviews_str.upper().endswith('M'):
        try:
            num = float(reviews_str[:-1])
            return int(num * 1000000)
        except ValueError:
            return 0
    else:
        try:
            return int(float(reviews_str))
        except ValueError:
            return 0


def parse_price_from_extensions(extensions_str: str) -> tuple[float, str]:
    """Parse price from extensions string like "Current price: $2"."""
    if not extensions_str or not isinstance(extensions_str, str):
        return None, ""
    
    price_match = re.search(r'Current price:\s*\$?(\d+(?:\.\d+)?)', extensions_str)
    if price_match:
        try:
            price_val = float(price_match.group(1))
            return price_val, f"${price_val}"
        except ValueError:
            pass
    
    price_match = re.search(r'\$(\d+(?:\.\d+)?)', extensions_str)
    if price_match:
        try:
            price_val = float(price_match.group(1))
            return price_val, f"${price_val}"
        except ValueError:
            pass
    
    return None, extensions_str


def construct_shopping_link(product_id: str) -> str:
    """Construct a Google Shopping link from product ID."""
    if not product_id:
        return ""
    return f"https://www.google.com.au/shopping/product/{product_id}"


def _is_number(val: Any) -> bool:
    """Return True if val can be safely cast to float."""
    try:
        float(val)
        return True
    except Exception:
        return False


def fetch_popular_products_scrapingdog(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Fetches popular products using ScrapingDog API.
    """
    country_code = "au" if location == "Australia" else "us"
    
    params = {
        "api_key": SCRAPINGDOG_API_KEY,
        "query": keyword,
        "page": 0,
        "country": country_code,
        "results": 10,
        "advance_search": "true",
        "ai_overview": "false"
    }

    try:
        response = requests.get(SCRAPINGDOG_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        popular = data.get("popular_products", [])[:top_n]
        scrape_date = datetime.datetime.utcnow().isoformat()
        records = []

        for i, item in enumerate(popular):
            product_id = str(item.get("product_id", "")) if item.get("product_id") else ""
            link = construct_shopping_link(product_id) if product_id else ""
            reviews_count = parse_reviews_count(item.get("reviews", ""))
            
            price_val = None
            price_raw_val = ""
            if item.get("price") and _is_number(item["price"]):
                price_val = float(item["price"])
                price_raw_val = f"${price_val}"
            else:
                price_val, price_raw_val = parse_price_from_extensions(item.get("extensions", ""))

            records.append({
                "scrape_date": scrape_date,
                "keyword": keyword,
                "position": i + 1,
                "product_id": product_id,
                "title": item.get("title", "") or "",
                "link": link,
                "rating": None,  # Not available in ScrapingDog
                "reviews": reviews_count,
                "price": price_val,
                "price_raw": price_raw_val,
                "merchant": item.get("seller", "") or "",
                "is_carousel": False,
                "carousel_position": None,
                "has_product_page": bool(link),
                "filters_raw": "",
                "api_source": "scrapingdog"  # Track which API was used
            })

        return records

    except Exception as e:
        # Silently fail and let the main function handle logging
        return []


def fetch_popular_products_valueserp(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Fetches popular products using ValueSERP API as fallback.
    """
    params = {
        "api_key": VALUESERP_API_KEY,
        "q": keyword,
        "location": location,
        "gl": "au",
        "hl": "en",
        "google_domain": "google.com.au",
        "include_ai_overview": "false",
        "ads_optimized": "false",
        "engine": "google",
    }

    try:
        response = requests.get(VALUESERP_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        popular = data.get("popular_products", [])[:top_n]
        scrape_date = datetime.datetime.utcnow().isoformat()
        records = []

        for item in popular:
            link = (
                item.get("link")
                or item.get("product_link")
                or item.get("url")
                or item.get("shopping_link")
                or ""
            )

            pid = extract_product_id_from_link(link) or ""
            if not pid and item.get("id"):
                pid = str(item["id"])

            rat = None
            if item.get("rating") is not None and _is_number(item["rating"]):
                rat = float(item["rating"])

            rev = 0
            if item.get("reviews") is not None and _is_number(item["reviews"]):
                rev = int(float(item["reviews"]))

            price_val = None
            price_raw_val = ""
            if item.get("price") is not None:
                if _is_number(item["price"]):
                    price_val = float(item["price"])
                    price_raw_val = str(item["price"])
                else:
                    price_raw_val = str(item["price"])
            elif isinstance(item.get("regular_price"), dict):
                val = item["regular_price"].get("value")
                if val is not None and _is_number(val):
                    price_val = float(val)
                    symbol = item["regular_price"].get("symbol", "")
                    price_raw_val = f"{symbol}{val}"

            records.append({
                "scrape_date": scrape_date,
                "keyword": keyword,
                "position": item.get("position") or 0,
                "product_id": pid,
                "title": item.get("title", "") or "",
                "link": link,
                "rating": rat,
                "reviews": rev,
                "price": price_val,
                "price_raw": price_raw_val,
                "merchant": item.get("merchant", "") or "",
                "is_carousel": False,
                "carousel_position": None,
                "has_product_page": bool(link),
                "filters_raw": "",
                "api_source": "valueserp"  # Track which API was used
            })

        return records

    except Exception as e:
        # Silently fail and let the main function handle logging
        return []


def fetch_popular_products(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Main function that tries ScrapingDog first, then ValueSERP as fallback.
    Uses quiet logging to avoid parallel processing confusion.
    """
    # Try ScrapingDog first (silently)
    results = fetch_popular_products_scrapingdog(keyword, top_n, location)
    
    if results:
        time.sleep(RATE_LIMIT_DELAY)
        return results
    
    # Fallback to ValueSERP if ScrapingDog found nothing (silently)
    results = fetch_popular_products_valueserp(keyword, top_n, location)
    
    time.sleep(RATE_LIMIT_DELAY)
    return results


def process_keyword(keyword: str, top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Wrapper around fetch_popular_products() with clean, concise logging.
    """
    start_time = time.time()
    try:
        results = fetch_popular_products(keyword, top_n=top_n, location=location)
        elapsed = time.time() - start_time
        
        if results:
            api_used = results[0].get('api_source', 'unknown')
            status_icon = "✅" if api_used == 'scrapingdog' else "🔄"
            print(f"{status_icon} {keyword:<20} → {len(results)} products ({elapsed:.1f}s)")
        else:
            print(f"❌ {keyword:<20} → 0 products ({elapsed:.1f}s)")
        
        return results

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"⚠️ {keyword:<20} → ERROR: {str(e)[:30]}... ({elapsed:.1f}s)")
        return []


def scrape_for_keywords(
    keywords: list[str],
    top_n: int = 10,
    location: str = "Australia",
    parallel: bool = True
) -> list[dict]:
    """
    Runs fetch_popular_products() for each keyword with fallback logic.
    """
    all_records: list[dict] = []
    total_start = time.time()

    print(f"\n🔍 Popular Products Scraper (Dual-API)")
    print(f"Keywords: {len(keywords)} | Parallel: {'Yes' if parallel else 'No'} | Delay: {RATE_LIMIT_DELAY}s")
    print("-" * 60)

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
    
    # Summary of API usage
    scrapingdog_count = sum(1 for r in all_records if r.get('api_source') == 'scrapingdog')
    valueserp_count = sum(1 for r in all_records if r.get('api_source') == 'valueserp')
    
    print("-" * 60)
    print(f"✅ Completed in {total_elapsed:.1f}s | Products: {len(all_records):,}")
    print(f"   ScrapingDog: {scrapingdog_count} | ValueSERP: {valueserp_count}")
    
    if keywords:
        success_rate = (len([r for r in all_records if r]) / len(keywords)) * 100
        print(f"   Success rate: {success_rate:.1f}% ({len(set(r['keyword'] for r in all_records))}/{len(keywords)} keywords)\n")
    else:
        print()

    return all_records


# If you run this file directly, you'll see a quick smoke test + debug output:
if __name__ == "__main__":
    sample_keywords = ["mens running shoes", "ghanda t-shirts", "woolworths groceries"]

    print("\n--- TESTING FALLBACK MECHANISM ---")
    results = scrape_for_keywords(sample_keywords, parallel=False)

    print(f"\nTotal found: {len(results)} products")
    
    # Show API breakdown
    apis_used = {}
    for result in results:
        api = result.get('api_source', 'unknown')
        apis_used[api] = apis_used.get(api, 0) + 1
    
    print("API Usage breakdown:")
    for api, count in apis_used.items():
        print(f"  {api.upper()}: {count} products")
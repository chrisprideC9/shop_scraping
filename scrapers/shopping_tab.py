import os
import time
import datetime
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("VALUESERP_API_KEY", "6CE8C7D87B3D43F98E6FE54728A1E39C")
BASE_URL = "https://api.valueserp.com/search"

# Configuration options
SKIP_US_FILTERS = os.getenv("SKIP_US_FILTERS", "false").lower() == "true"
PARALLEL_REQUESTS = int(os.getenv("PARALLEL_REQUESTS", "3"))  # Number of concurrent requests
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "0.5"))  # Delay between requests

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


def extract_product_id_from_link(link: str) -> str:
    """
    Extracts product ID from Google Shopping product links.
    """
    if not link:
        return None
    
    pattern = r'/shopping/product/(\d+)'
    match = re.search(pattern, link)
    
    if match:
        return match.group(1)
    
    parts = link.rstrip('/').split('/')
    if parts and parts[-1].isdigit():
        return parts[-1]
    
    return None


def fetch_aus_results(keyword: str) -> list[dict]:
    """
    Fetches shopping results for a keyword from the Australian SERP.
    """
    params = {**AUS_PARAMS, "api_key": API_KEY, "q": keyword}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    shopping_results = data.get("shopping_results", [])
    
    # Debug: Print the structure of shopping results (only for first keyword)
    if shopping_results and keyword == shopping_results[0].get('keyword', keyword):
        print(f"\nDEBUG - Shopping results for '{keyword}':")
        print(f"Found {len(shopping_results)} products")
        print("-" * 50)
    
    return shopping_results


def fetch_us_filters(keyword: str) -> str:
    """
    Fetches filter options for a keyword from the US SERP and concatenates them.
    """
    if SKIP_US_FILTERS:
        return ""
        
    params = {**US_PARAMS, "api_key": API_KEY, "q": keyword}
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
    Process a single keyword and return its results.
    """
    timestamp = datetime.datetime.utcnow().isoformat()
    results = []
    
    try:
        print(f"Processing: {keyword}")
        start_time = time.time()
        
        # Fetch Australian results
        aus_results = fetch_aus_results(keyword)
        
        # Fetch US filters (can be skipped)
        filters = fetch_us_filters(keyword) if not SKIP_US_FILTERS else ""
        
        # Process results
        for item in aus_results:
            link = item.get("link", "")
            product_id = item.get("id")
            if not product_id and link:
                product_id = extract_product_id_from_link(link)
            
            record = {
                "keyword": keyword, 
                "date": timestamp, 
                "filters": filters,
                "product_id": product_id
            }
            
            # Copy all fields except 'image' and 'id'
            for k, v in item.items():
                if k.lower() == "image" or k == "id":
                    continue
                record[k] = v
                
            results.append(record)
        
        elapsed = time.time() - start_time
        print(f"✓ Completed {keyword} in {elapsed:.2f}s - Found {len(results)} products")
        
    except requests.HTTPError as err:
        print(f"✗ HTTP error for '{keyword}': {err}")
    except Exception as e:
        print(f"✗ Error processing '{keyword}': {e}")
    
    # Rate limiting
    time.sleep(RATE_LIMIT_DELAY)
    
    return results


def scrape_shopping_tab_for_keywords(keywords: list[str], parallel: bool = True) -> list[dict]:
    """
    For each keyword, fetch AUS shopping results and US filters.
    
    Args:
        keywords: List of keywords to search
        parallel: Whether to use parallel processing (default: True)
    
    Returns:
        List of all product records
    """
    all_records = []
    total_start = time.time()
    
    print(f"\nStarting shopping tab scraper for {len(keywords)} keywords")
    print(f"Parallel processing: {'Yes' if parallel else 'No'}")
    print(f"Skip US filters: {'Yes' if SKIP_US_FILTERS else 'No'}")
    print("="*60)
    
    if parallel and len(keywords) > 1:
        # Process keywords in parallel
        with ThreadPoolExecutor(max_workers=PARALLEL_REQUESTS) as executor:
            future_to_keyword = {
                executor.submit(process_keyword, kw): kw 
                for kw in keywords
            }
            
            for future in as_completed(future_to_keyword):
                keyword = future_to_keyword[future]
                try:
                    results = future.result()
                    all_records.extend(results)
                except Exception as e:
                    print(f"✗ Failed to process '{keyword}': {e}")
    else:
        # Sequential processing
        for keyword in keywords:
            results = process_keyword(keyword)
            all_records.extend(results)
    
    total_elapsed = time.time() - total_start
    print("="*60)
    print(f"Completed! Total time: {total_elapsed:.2f}s")
    print(f"Total products found: {len(all_records)}")
    print(f"Average time per keyword: {total_elapsed/len(keywords):.2f}s")
    
    return all_records


if __name__ == "__main__":
    # Test with sample keywords
    sample_keywords = ["mens running shoes", "wool knit jumper", "laptop bag"]
    
    # Test sequential
    print("\n--- SEQUENTIAL TEST ---")
    results_seq = scrape_shopping_tab_for_keywords(sample_keywords, parallel=False)
    
    # Test parallel
    print("\n--- PARALLEL TEST ---")
    results_par = scrape_shopping_tab_for_keywords(sample_keywords, parallel=True)
    
    print(f"\nSequential found: {len(results_seq)} products")
    print(f"Parallel found: {len(results_par)} products")
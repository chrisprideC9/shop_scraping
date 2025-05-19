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
    Fetches the top N shopping results for a given keyword using the ValueSERP API.

    Args:
        keyword: The search query.
        top_n: Number of top results to return.
        location: Geographical location parameter for ValueSERP.

    Returns:
        A list of dicts, each containing:
          - date (ISO string)
          - keyword
          - title, link, price, merchant, delivery (if available)
    """
    params = {
        "api_key": API_KEY,
        "search_type": "shopping",
        "q": keyword,
        "location": location
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    raw_results = data.get("shopping_results", [])[:top_n]
    today = datetime.date.today().isoformat()
    records = []

    for item in raw_results:
        records.append({
            "date": today,
            "keyword": keyword,
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "price": item.get("price", ""),
            "merchant": item.get("merchant", ""),
            "delivery": item.get("delivery", "")
        })

    # Respect API rate limits
    time.sleep(1)

    return records


def scrape_for_keywords(keywords: list[str], top_n: int = 10, location: str = "Australia") -> list[dict]:
    """
    Runs fetch_popular_products for each keyword in the list.

    Args:
        keywords: List of search queries.

    Returns:
        Combined list of all records.
    """
    all_results = []
    for kw in keywords:
        try:
            results = fetch_popular_products(kw, top_n=top_n, location=location)
            all_results.extend(results)
        except requests.HTTPError as e:
            print(f"HTTP error for '{kw}': {e}")
        except Exception as e:
            print(f"Error fetching results for '{kw}': {e}")
    return all_results


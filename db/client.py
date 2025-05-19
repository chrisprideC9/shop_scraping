import os
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


def get_connection():
    """
    Establishes and returns a new database connection.
    """
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursor_factory=DictCursor
    )


def get_active_campaigns():
    query = """
        SELECT campaign_id, domain_name
        FROM campaigns
        WHERE scrape_value = TRUE;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
    return [dict(row) for row in results]


def get_keywords_for_campaign(campaign_id):
    """
    Fetches keywords associated with a given campaign_id.
    Returns a list of keyword strings.
    """
    query = """
        SELECT keyword
        FROM shopping_scrape_keywords
        WHERE campaign_id = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (campaign_id,))
            rows = cur.fetchall()
    return [row['keyword'] for row in rows]


if __name__ == "__main__":
    # Quick test
    campaigns = get_active_campaigns()
    print("Active campaigns:", campaigns)
    if campaigns:
        first_id = campaigns[0]['campaign_id']
        keywords = get_keywords_for_campaign(first_id)
        print(f"Keywords for campaign {first_id}: {keywords}")

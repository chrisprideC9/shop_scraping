import re
from typing import List, Dict, Tuple, Any
from psycopg2 import sql
from db.client import get_connection


def parse_filters_raw(filters_raw: str) -> List[Tuple[str, str]]:
    """
    Splits a raw filters string into a list of (category, value) tuples.
    """
    if not filters_raw or not isinstance(filters_raw, str):
        return []
    parts = [p.strip() for p in filters_raw.split(',') if p.strip()]
    pairs = []
    for part in parts:
        if ' - ' in part:
            cat, val = part.split(' - ', 1)
            pairs.append((cat.strip(), val.strip()))
    return pairs


def upload_scrape_data(
    records: List[Dict[str, Any]],
    campaign_id: int,
    scrape_type_id: int
) -> List[int]:
    """
    Inserts scraped data into scrape_data and corresponding filters into scrape_data_filter.

    Args:
      records: List of dicts, where each dict contains keys matching columns of scrape_data,
               optionally including 'filters_raw'.
      campaign_id: The campaign_id to tag each record with.
      scrape_type_id: The scrape_type_id to tag each record with.

    Returns:
      A list of the newly inserted scrape_data IDs.
    """
    inserted_ids: List[int] = []

    insert_cols = [
        'campaign_id', 'scrape_type_id', 'scrape_date', 'keyword',
        'position', 'product_id', 'title', 'link', 'rating', 'reviews',
        'price', 'price_raw', 'merchant', 'is_carousel', 'carousel_position', 'has_product_page'
    ]

    # Build the INSERT statement dynamically
    col_identifiers = [sql.Identifier(col) for col in insert_cols]
    placeholders = [sql.Placeholder()]*len(insert_cols)
    insert_stmt = sql.SQL("""
        INSERT INTO scrape_data ({fields})
        VALUES ({values})
        RETURNING id
    """).format(
        fields=sql.SQL(',').join(col_identifiers),
        values=sql.SQL(',').join(placeholders)
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            for rec in records:
                # Prepare values for the main scrub_data table
                values = [
                    campaign_id,
                    scrape_type_id,
                    rec.get('date') or rec.get('scrape_date'),
                    rec.get('keyword'),
                    rec.get('position'),
                    rec.get('product_id'),
                    rec.get('title'),
                    rec.get('link'),
                    rec.get('rating'),
                    rec.get('reviews'),
                    rec.get('price'),
                    rec.get('price_raw'),
                    rec.get('merchant'),
                    rec.get('is_carousel', False),
                    rec.get('carousel_position'),
                    rec.get('has_product_page', False)
                ]

                cur.execute(insert_stmt, values)
                new_id = cur.fetchone()[0]
                inserted_ids.append(new_id)

                # Handle filters if present
                raw = rec.get('filters_raw')
                for category, val in parse_filters_raw(raw):
                    cur.execute(
                        "INSERT INTO scrape_data_filter (scrape_data_id, filter_category, filter_value) VALUES (%s, %s, %s)",
                        (new_id, category, val)
                    )

        conn.commit()
    return inserted_ids

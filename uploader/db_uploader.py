# ── uploader/db_uploader.py ──

import re
import math
from typing import List, Dict, Tuple, Any
from psycopg2 import sql
from psycopg2.extras import execute_values
from db.client import get_connection


def parse_filters_raw(filters_raw: str) -> List[Tuple[str, str]]:
    """
    Splits a raw filters string into a list of (category, value) tuples.
    """
    if not filters_raw or not isinstance(filters_raw, str):
        return []
    parts = [p.strip() for p in filters_raw.split(',') if p.strip()]
    pairs: List[Tuple[str, str]] = []
    for part in parts:
        if ' - ' in part:
            cat, val = part.split(' - ', 1)
            pairs.append((cat.strip(), val.strip()))
    return pairs


def _none_if_nan(val: Any) -> Any:
    """
    If val is a float NaN, return None; otherwise return val unchanged.
    """
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def upload_scrape_data(
    records: List[Dict[str, Any]],
    campaign_id: int,
    scrape_type_id: int
) -> List[int]:
    """
    Inserts scraped data into scrape_data and corresponding filters into scrape_data_filter
    using two bulk-insert statements. Converts NaN → None and empty-string product_id/link → None
    so that columns declared NULLable accept them. Uses execute_values(fetch=True) to retrieve
    all RETURNING ids from every page of the batch.
    """
    inserted_ids: List[int] = []

    # 1) Build tuples for scrape_data insertion
    main_insert_values: List[Tuple[Any, ...]] = []
    for rec in records:
        scrape_date = rec.get('date') or rec.get('scrape_date')

        # Convert empty-string product_id → None
        pid_raw = rec.get('product_id')
        pid = None if pid_raw == "" else pid_raw

        # Convert empty-string link → None
        link_raw = rec.get('link') or ""
        link = None if link_raw == "" else link_raw

        # Convert NaN numeric fields → None
        kw          = rec.get('keyword')
        pos         = _none_if_nan(rec.get('position'))
        title       = rec.get('title') or ""
        rating_val  = _none_if_nan(rec.get('rating'))
        reviews_val = _none_if_nan(rec.get('reviews'))
        price_val   = _none_if_nan(rec.get('price'))
        price_raw   = rec.get('price_raw') or ""
        merchant    = rec.get('merchant') or ""
        is_car      = rec.get('is_carousel', False)
        carousel_p  = rec.get('carousel_position')
        has_prod    = rec.get('has_product_page', False)

        main_insert_values.append(
            (
                campaign_id,
                scrape_type_id,
                scrape_date,
                kw,
                pos,
                pid,      # possibly None
                title,
                link,     # possibly None
                rating_val,
                reviews_val,
                price_val,
                price_raw,
                merchant,
                is_car,
                carousel_p,
                has_prod,
            )
        )

    # If there’s nothing to insert, return immediately
    if not main_insert_values:
        return []

    # 2) Prepare the bulk INSERT … RETURNING id
    insert_cols = [
        'campaign_id', 'scrape_type_id', 'scrape_date', 'keyword',
        'position', 'product_id', 'title', 'link', 'rating', 'reviews',
        'price', 'price_raw', 'merchant', 'is_carousel', 'carousel_position', 'has_product_page'
    ]
    col_identifiers = [sql.Identifier(col) for col in insert_cols]
    insert_query_main = sql.SQL("""
        INSERT INTO scrape_data ({fields})
        VALUES %s
        RETURNING id
    """).format(fields=sql.SQL(', ').join(col_identifiers))

    # 3) Open ONE connection & cursor, run the bulk insert
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Use fetch=True so that execute_values collects RETURNING ids from every page
            returned = execute_values(
                cur,
                insert_query_main,
                main_insert_values,
                fetch=True
            )

            # 4) Normalize returned into a flat list of ints
            normalized_ids: List[int] = []
            if isinstance(returned, list):
                # If returned is a list of pages (each page is a list of tuples or ints)
                if returned and isinstance(returned[0], list):
                    for page in returned:
                        for item in page:
                            if isinstance(item, tuple):
                                normalized_ids.append(item[0])
                            elif isinstance(item, int):
                                normalized_ids.append(item)
                else:
                    # Single page; returned is a list of tuples or ints
                    for item in returned:
                        if isinstance(item, tuple):
                            normalized_ids.append(item[0])
                        elif isinstance(item, int):
                            normalized_ids.append(item)
            elif isinstance(returned, tuple):
                # Single row inserted
                normalized_ids.append(returned[0])
            elif isinstance(returned, int):
                normalized_ids.append(returned)

            inserted_ids = normalized_ids

            # 5) Sanity check: if counts differ, dump debug info and raise
            if len(inserted_ids) != len(records):
                print(
                    f"⚠️ MISMATCH: tried to insert {len(records)} rows, "
                    f"but got only {len(inserted_ids)} IDs back."
                )
                print("▶ First 5 tuples passed to INSERT (main_insert_values[0..4]):")
                for idx in range(min(5, len(main_insert_values))):
                    print(f"   idx={idx}: {main_insert_values[idx]}")
                print("▶ First 5 raw record-dicts:")
                for idx in range(min(5, len(records))):
                    print(f"   idx={idx}: {records[idx]}")
                raise RuntimeError(
                    f"Expected {len(records)} IDs but got {len(inserted_ids)}. "
                    "Check the debug output above for the first few failing rows."
                )

            # 6) Build filter rows by zipping records ↔ inserted_ids
            filter_rows: List[Tuple[int, str, str]] = []
            for rec, new_id in zip(records, inserted_ids):
                raw = rec.get('filters_raw')
                for category, val in parse_filters_raw(raw):
                    filter_rows.append((new_id, category, val))

            # 7) Bulk-insert all filters in one go (if any)
            if filter_rows:
                insert_query_filters = """
                    INSERT INTO scrape_data_filter
                      (scrape_data_id, filter_category, filter_value)
                    VALUES %s
                """
                execute_values(cur, insert_query_filters, filter_rows)

        # 8) Commit once for both tables
        conn.commit()

    return inserted_ids

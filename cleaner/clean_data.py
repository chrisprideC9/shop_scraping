import pandas as pd


def clean_popular_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and normalises a DataFrame of popular_products records.

    Input keys: scrape_date, keyword, position, product_id,
                title, link, rating, reviews, price, price_raw,
                merchant, is_carousel, carousel_position, has_product_page

    Returns DataFrame with exactly these columns, filling missing with None:
      [scrape_date, keyword, position, product_id, title, link,
       rating, reviews, price, price_raw, merchant,
       is_carousel, carousel_position, has_product_page]
    """
    expected_cols = [
        'scrape_date', 'keyword', 'position', 'product_id',
        'title', 'link', 'rating', 'reviews', 'price',
        'price_raw', 'merchant', 'is_carousel', 'carousel_position',
        'has_product_page'
    ]
    df_clean = df.copy()
    
    # Check if 'date' exists but 'scrape_date' doesn't, and rename it
    if 'date' in df_clean.columns and 'scrape_date' not in df_clean.columns:
        df_clean['scrape_date'] = df_clean['date']
        df_clean = df_clean.drop(columns=['date'])

    # Ensure all expected columns exist
    for col in expected_cols:
        if col not in df_clean.columns:
            df_clean[col] = None

    # Parse scrape_date
    df_clean['scrape_date'] = pd.to_datetime(df_clean['scrape_date'], errors='coerce')

    # Numeric conversions
    df_clean['position'] = pd.to_numeric(df_clean['position'], errors='coerce', downcast='integer')
    df_clean['reviews'] = pd.to_numeric(df_clean['reviews'], errors='coerce', downcast='integer')
    df_clean['rating'] = pd.to_numeric(df_clean['rating'], errors='coerce')
    df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')

    # Boolean conversions
    df_clean['is_carousel'] = df_clean['is_carousel'].astype(bool)
    df_clean['has_product_page'] = df_clean['has_product_page'].astype(bool)

    # Reorder columns
    return df_clean[expected_cols]


def clean_shopping_tab(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a raw DataFrame from the shopping_tab scraper.
    """
    df_clean = df.copy()

    # Rename and parse date
    if 'Date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['Date'], dayfirst=True, errors='coerce')
        df_clean = df_clean.drop(columns=['Date'])

    # Normalize column names
    df_clean.columns = [c.strip().lower().replace(' ', '_') for c in df_clean.columns]
    if 'query' in df_clean.columns and 'keyword' not in df_clean.columns:
        df_clean = df_clean.rename(columns={'query': 'keyword'})

    # Numeric fields
    if 'price' in df_clean.columns:
        df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
    if 'position' in df_clean.columns:
        df_clean['position'] = pd.to_numeric(df_clean['position'], errors='coerce', downcast='integer')

    # Rename filters to filters_raw
    if 'filters' in df_clean.columns:
        df_clean = df_clean.rename(columns={'filters': 'filters_raw'})

    # Drop image if present
    df_clean = df_clean.loc[:, [c for c in df_clean.columns if c != 'image']]

    # Order columns
    cols = [
        'date', 'keyword', 'position', 'product_id',
        'title', 'link', 'price', 'merchant', 'filters_raw'
    ]
    return df_clean[[c for c in cols if c in df_clean.columns]]


def extract_filters(df_cleaned: pd.DataFrame) -> pd.DataFrame:
    """
    From a cleaned shopping_tab DataFrame, extracts filters.
    """
    records = []
    for _, row in df_cleaned.iterrows():
        raw = row.get('filters_raw', '')
        if pd.isna(raw) or not raw:
            continue
        parts = [p.strip() for p in str(raw).split(',') if p.strip()]
        for part in parts:
            if ' - ' in part:
                cat, val = part.split(' - ', 1)
                records.append({
                    'keyword': row.get('keyword'),
                    'position': row.get('position'),
                    'filter_category': cat.strip(),
                    'filter_value': val.strip()
                })
    return pd.DataFrame.from_records(records)

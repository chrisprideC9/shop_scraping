import logging
import pandas as pd

from db.client import get_active_campaigns, get_keywords_for_campaign
from scrapers.popular_products import scrape_for_keywords as popular_scrape
from scrapers.shopping_tab import scrape_shopping_tab_for_keywords
from cleaner.clean_data import clean_popular_products, clean_shopping_tab
from uploader.db_uploader import upload_scrape_data


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    campaigns = get_active_campaigns()
    logging.info(f"Found {len(campaigns)} active campaigns to process.")

    for campaign in campaigns:
        campaign_id = campaign['campaign_id']
        domain = campaign.get('domain_name')

        keywords = get_keywords_for_campaign(campaign_id)
        if not keywords:
            logging.warning(f"Campaign {campaign_id} ({domain}) has no keywords. Skipping.")
            continue

        logging.info(f"Campaign {campaign_id} ({domain}): {len(keywords)} keywords.")

        # Run popular products scraper (scrape_type_id=1)
        logging.info(f"Running popular-products scraper for campaign {campaign_id}.")
        raw_popular = popular_scrape(keywords)
        df_popular = pd.DataFrame(raw_popular)
        df_popular_clean = clean_popular_products(df_popular)

        if not df_popular_clean.empty:
            inserted_popular = upload_scrape_data(df_popular_clean.to_dict(orient='records'), campaign_id, scrape_type_id=1)
            logging.info(f"Inserted {len(inserted_popular)} rows for popular products.")

        # Run shopping tab scraper (scrape_type_id=2)
        logging.info(f"Running shopping-tab scraper for campaign {campaign_id}.")
        raw_shopping_tab = scrape_shopping_tab_for_keywords(keywords)
        df_shopping_tab = pd.DataFrame(raw_shopping_tab)
        df_shopping_tab_clean = clean_shopping_tab(df_shopping_tab)

        if not df_shopping_tab_clean.empty:
            inserted_shopping = upload_scrape_data(df_shopping_tab_clean.to_dict(orient='records'), campaign_id, scrape_type_id=2)
            logging.info(f"Inserted {len(inserted_shopping)} rows for shopping tab.")

    logging.info("All campaigns processed.")


if __name__ == "__main__":
    main()

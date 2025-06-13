import logging
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

from db.client import get_active_campaigns, get_keywords_for_campaign
from scrapers.popular_products import scrape_for_keywords as popular_scrape
from scrapers.shopping_tab import scrape_shopping_tab_for_keywords
from cleaner.clean_data import clean_popular_products, clean_shopping_tab
from uploader.db_uploader import upload_scrape_data
from notifications.slack_notifier import send_scraping_summary


class ScrapingSummary:
    """Helper class to collect and track scraping statistics."""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        self.campaigns_processed: List[Dict[str, Any]] = []
        self.errors: List[str] = []
        self.total_popular_products = 0
        self.total_shopping_products = 0
        self.total_keywords = 0
    
    def add_campaign(self, campaign_id: int, domain: str, keyword_count: int, keywords: List[str]):
        """Add a new campaign to track."""
        self.campaigns_processed.append({
            'campaign_id': campaign_id,
            'domain': domain,
            'keywords': keywords,
            'keyword_count': keyword_count,
            'popular_products_found': 0,
            'shopping_products_found': 0,
            'keywords_with_no_popular': [],
            'sample_products': [],
            'api_usage': '',
            'start_time': datetime.now()
        })
        self.total_keywords += keyword_count
    
    def update_campaign_results(self, campaign_id: int, popular_count: int, shopping_count: int, 
                              keywords_no_popular: List[str] = None, sample_products: List[str] = None,
                              api_usage: str = None):
        """Update results for a campaign."""
        for campaign in self.campaigns_processed:
            if campaign['campaign_id'] == campaign_id:
                campaign['popular_products_found'] = popular_count
                campaign['shopping_products_found'] = shopping_count
                campaign['keywords_with_no_popular'] = keywords_no_popular or []
                campaign['sample_products'] = sample_products or []
                campaign['api_usage'] = api_usage or ""
                campaign['end_time'] = datetime.now()
                break
        
        self.total_popular_products += popular_count
        self.total_shopping_products += shopping_count
    
    def add_error(self, error_message: str):
        """Add an error to the summary."""
        self.errors.append(error_message)
        logging.error(error_message)
    
    def finalize(self):
        """Finalize the summary with end time and performance metrics."""
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        # Calculate performance metrics
        self.performance_metrics = {
            'duration_seconds': duration,
            'avg_time_per_keyword': duration / max(self.total_keywords, 1),
            'products_per_minute': (self.total_popular_products + self.total_shopping_products) / (duration / 60) if duration > 0 else 0,
            'keywords_per_minute': self.total_keywords / (duration / 60) if duration > 0 else 0
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary for Slack notification."""
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time else 0,
            'campaigns_processed': self.campaigns_processed,
            'errors': self.errors,
            'total_keywords': self.total_keywords,
            'total_popular_products': self.total_popular_products,
            'total_shopping_products': self.total_shopping_products,
            'performance_metrics': getattr(self, 'performance_metrics', {})
        }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Initialize summary tracking
    summary = ScrapingSummary()
    
    try:
        campaigns = get_active_campaigns()
        logging.info(f"Found {len(campaigns)} active campaigns to process.")
        
        if not campaigns:
            summary.add_error("No active campaigns found to process")
            summary.finalize()
            send_scraping_summary(summary.to_dict())
            return

        for campaign in campaigns:
            campaign_id = campaign['campaign_id']
            domain = campaign.get('domain_name', 'Unknown')
            
            try:
                keywords = get_keywords_for_campaign(campaign_id)
                if not keywords:
                    error_msg = f"Campaign {campaign_id} ({domain}) has no keywords. Skipping."
                    summary.add_error(error_msg)
                    continue

                logging.info(f"Campaign {campaign_id} ({domain}): {len(keywords)} keywords.")
                summary.add_campaign(campaign_id, domain, len(keywords), keywords)
                
                popular_count = 0
                shopping_count = 0
                keywords_with_no_popular = []
                sample_products = []
                api_usage_str = ""

                # Run popular products scraper (scrape_type_id=1)
                try:
                    logging.info(f"Running popular-products scraper for {domain}.")
                    start_time = time.time()
                    
                    raw_popular = popular_scrape(keywords)
                    df_popular = pd.DataFrame(raw_popular)
                    
                    if not df_popular.empty:
                        df_popular_clean = clean_popular_products(df_popular)
                        
                        # Track keywords with no popular products and API usage
                        keywords_with_results = set(df_popular_clean['keyword'].unique())
                        keywords_with_no_popular = [kw for kw in keywords if kw not in keywords_with_results]
                        
                        # Get sample product titles (first 5 unique titles)
                        sample_products = df_popular_clean['title'].dropna().unique()[:5].tolist()
                        
                        # Track API usage for this campaign
                        api_usage = df_popular_clean['api_source'].value_counts().to_dict() if 'api_source' in df_popular_clean.columns else {}
                        api_usage_str = ", ".join([f"{api.upper()}: {count}" for api, count in api_usage.items()]) if api_usage else "Unknown"
                        
                        inserted_popular = upload_scrape_data(df_popular_clean.to_dict(orient='records'), campaign_id, scrape_type_id=1)
                        popular_count = len(inserted_popular)
                        
                        elapsed = time.time() - start_time
                        logging.info(f"✓ Popular products: {popular_count} rows inserted in {elapsed:.1f}s ({api_usage_str})")
                        
                        if keywords_with_no_popular:
                            logging.info(f"⚠️ {len(keywords_with_no_popular)} keywords had no popular products: {', '.join(keywords_with_no_popular[:3])}{'...' if len(keywords_with_no_popular) > 3 else ''}")
                    else:
                        logging.warning(f"No popular products found for {domain}")
                        keywords_with_no_popular = keywords  # All keywords had no results
                        
                except Exception as e:
                    error_msg = f"Popular products scraper failed for {domain}: {str(e)}"
                    summary.add_error(error_msg)
                    keywords_with_no_popular = keywords  # Assume all failed

                # Run shopping tab scraper (scrape_type_id=2)
                try:
                    logging.info(f"Running shopping-tab scraper for {domain}.")
                    start_time = time.time()
                    
                    raw_shopping_tab = scrape_shopping_tab_for_keywords(keywords)
                    df_shopping_tab = pd.DataFrame(raw_shopping_tab)
                    
                    if not df_shopping_tab.empty:
                        df_shopping_tab_clean = clean_shopping_tab(df_shopping_tab)
                        inserted_shopping = upload_scrape_data(df_shopping_tab_clean.to_dict(orient='records'), campaign_id, scrape_type_id=2)
                        shopping_count = len(inserted_shopping)
                        
                        elapsed = time.time() - start_time
                        logging.info(f"✓ Shopping tab: {shopping_count} rows inserted in {elapsed:.1f}s")
                    else:
                        logging.warning(f"No shopping tab products found for {domain}")
                        
                except Exception as e:
                    error_msg = f"Shopping tab scraper failed for {domain}: {str(e)}"
                    summary.add_error(error_msg)

                # Update campaign results
                summary.update_campaign_results(
                    campaign_id, 
                    popular_count, 
                    shopping_count, 
                    keywords_with_no_popular, 
                    sample_products,
                    api_usage_str
                )
                
                logging.info(f"✓ Campaign {domain} completed: {popular_count} popular + {shopping_count} shopping products")
                
            except Exception as e:
                error_msg = f"Campaign {campaign_id} ({domain}) failed completely: {str(e)}"
                summary.add_error(error_msg)
                continue

        logging.info("All campaigns processed.")
        
    except Exception as e:
        critical_error = f"Critical error in main process: {str(e)}"
        summary.add_error(critical_error)
        logging.critical(critical_error)
    
    finally:
        # Finalize summary and send Slack notification
        summary.finalize()
        
        # Log final summary
        total_duration = summary.performance_metrics.get('duration_seconds', 0)
        total_products = summary.total_popular_products + summary.total_shopping_products
        
        logging.info("=" * 60)
        logging.info("SCRAPING RUN SUMMARY")
        logging.info("=" * 60)
        logging.info(f"Duration: {total_duration:.1f}s")
        logging.info(f"Campaigns: {len(summary.campaigns_processed)}")
        logging.info(f"Keywords: {summary.total_keywords}")
        logging.info(f"Popular Products: {summary.total_popular_products:,}")
        logging.info(f"Shopping Products: {summary.total_shopping_products:,}")
        logging.info(f"Total Products: {total_products:,}")
        logging.info(f"Errors: {len(summary.errors)}")
        
        if summary.errors:
            logging.info("ERRORS:")
            for error in summary.errors:
                logging.info(f"  • {error}")
        
        logging.info("=" * 60)
        
        # Send Slack notification
        try:
            success = send_scraping_summary(summary.to_dict())
            if success:
                logging.info("✓ Slack notification sent successfully")
            else:
                logging.warning("✗ Failed to send Slack notification")
        except Exception as e:
            logging.error(f"Error sending Slack notification: {e}")


if __name__ == "__main__":
    main()
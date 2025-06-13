# ── notifications/slack_notifier.py ──

import os
import ssl
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

load_dotenv()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#scraping-reports")


class SlackNotifier:
    def __init__(self, token: str = None, channel: str = None):
        self.token = token or SLACK_BOT_TOKEN
        self.channel = channel or SLACK_CHANNEL
        
        if self.token:
            # Configure SSL context to handle certificate issues
            try:
                # Try to create a default SSL context
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                self.client = WebClient(
                    token=self.token,
                    ssl=ssl_context
                )
            except Exception as e:
                logging.warning(f"SSL context creation failed, using default client: {e}")
                # Fallback to default client
                self.client = WebClient(token=self.token)
        else:
            self.client = None
            logging.warning("Slack bot token not provided. Slack notifications disabled.")
    
    def send_scraping_summary(self, summary_data: Dict) -> bool:
        """
        Sends a formatted scraping summary to Slack.
        Uses fallback method if SSL issues occur.
        """
        if not self.token:
            logging.warning("Slack token not available. Skipping notification.")
            return False
        
        try:
            message_blocks = self._build_summary_blocks(summary_data)
            
            # First try with WebClient
            if self.client:
                try:
                    response = self.client.chat_postMessage(
                        channel=self.channel,
                        blocks=message_blocks,
                        text="Scraping Run Summary"
                    )
                    logging.info(f"Slack message sent successfully: {response['ts']}")
                    return True
                except Exception as e:
                    logging.warning(f"WebClient failed, trying requests fallback: {e}")
            
            # Fallback to direct requests if WebClient fails
            return self._send_via_requests(message_blocks)
            
        except Exception as e:
            logging.error(f"Unexpected error sending Slack message: {e}")
            return False
    
    def _send_via_requests(self, message_blocks: List[Dict]) -> bool:
        """
        Fallback method using requests directly to bypass SSL issues.
        """
        try:
            # Suppress SSL warnings when using verify=False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            url = "https://slack.com/api/chat.postMessage"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }
            payload = {
                "channel": self.channel,
                "blocks": message_blocks,
                "text": "Scraping Run Summary"
            }
            
            # Disable SSL verification as a workaround
            response = requests.post(
                url, 
                json=payload, 
                headers=headers, 
                verify=False,  # This bypasses SSL verification
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get("ok"):
                    logging.info("Slack message sent successfully via requests fallback")
                    return True
                else:
                    logging.error(f"Slack API error: {result.get('error')}")
                    return False
            else:
                logging.error(f"HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Requests fallback also failed: {e}")
            return False
    
    def _build_summary_blocks(self, data: Dict) -> List[Dict]:
        """
        Builds Slack block kit message from summary data.
        """
        # Header with timestamp
        start_time = data.get('start_time', datetime.now())
        end_time = data.get('end_time', datetime.now())
        duration = data.get('duration_seconds', 0)
        
        # Status emoji based on success/errors
        status_emoji = "✅" if data.get('errors', []) == [] else "⚠️"
        status_text = "Completed Successfully" if data.get('errors', []) == [] else "Completed with Issues"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} Shopping Scraper Summary"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:* {status_text}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Duration:* {self._format_duration(duration)}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Started:* {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Finished:* {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            },
            {
                "type": "divider"
            }
        ]
        
        # Campaign summary
        campaigns = data.get('campaigns_processed', [])
        total_campaigns = len(campaigns)
        total_keywords = sum(c.get('keyword_count', 0) for c in campaigns)
        total_popular_products = sum(c.get('popular_products_found', 0) for c in campaigns)
        total_shopping_products = sum(c.get('shopping_products_found', 0) for c in campaigns)
        
        # Calculate keywords with no popular products across all campaigns
        total_keywords_no_popular = sum(len(c.get('keywords_with_no_popular', [])) for c in campaigns)
        popular_success_rate = ((total_keywords - total_keywords_no_popular) / total_keywords * 100) if total_keywords > 0 else 0
        
        blocks.extend([
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Overall Summary*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Campaigns Processed:* {total_campaigns}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Total Keywords:* {total_keywords}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Popular Products Found:* {total_popular_products:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Shopping Products Found:* {total_shopping_products:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Keywords w/ No Popular:* {total_keywords_no_popular}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Popular Products Success:* {popular_success_rate:.1f}%"
                    }
                ]
            }
        ])
        
        # Campaign details
        if campaigns:
            blocks.extend([
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*🎯 Campaign Details*"
                    }
                }
            ])
            
            for campaign in campaigns[:5]:  # Show first 5 campaigns
                domain = campaign.get('domain', 'Unknown')
                keywords_count = campaign.get('keyword_count', 0)
                popular_count = campaign.get('popular_products_found', 0)
                shopping_count = campaign.get('shopping_products_found', 0)
                keywords_no_popular = campaign.get('keywords_with_no_popular', [])
                sample_products = campaign.get('sample_products', [])
                api_usage = campaign.get('api_usage', '')
                
                # Build campaign summary text
                campaign_text = f"*{domain}*\n• {keywords_count} keywords • {popular_count} popular products • {shopping_count} shopping products"
                
                # Add API usage if available
                if api_usage:
                    campaign_text += f"\n• 🔗 API usage: {api_usage}"
                
                # Add keywords with no popular products if any
                if keywords_no_popular:
                    no_popular_count = len(keywords_no_popular)
                    if no_popular_count <= 3:
                        campaign_text += f"\n• ⚠️ No popular products: {', '.join(keywords_no_popular)}"
                    else:
                        campaign_text += f"\n• ⚠️ {no_popular_count} keywords had no popular products: {', '.join(keywords_no_popular[:2])}..."
                
                # Add sample products if any
                if sample_products:
                    sample_count = len(sample_products)
                    if sample_count <= 2:
                        campaign_text += f"\n• 📦 Sample products: {', '.join(sample_products)}"
                    else:
                        campaign_text += f"\n• 📦 Sample products: {', '.join(sample_products[:2])}... (+{sample_count-2} more)"
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": campaign_text
                    }
                })
            
            if len(campaigns) > 5:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"_... and {len(campaigns) - 5} more campaigns_"
                    }
                })
        
        # Errors section
        errors = data.get('errors', [])
        if errors:
            blocks.extend([
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*⚠️ Issues ({len(errors)})*"
                    }
                }
            ])
            
            error_text = "\n".join([f"• {error}" for error in errors[:5]])
            if len(errors) > 5:
                error_text += f"\n_... and {len(errors) - 5} more issues_"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": error_text
                }
            })
        
        # Performance metrics
        if data.get('performance_metrics'):
            metrics = data['performance_metrics']
            blocks.extend([
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*⚡ Performance*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Avg Time/Keyword:* {metrics.get('avg_time_per_keyword', 0):.1f}s"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Products/Minute:* {metrics.get('products_per_minute', 0):.1f}"
                        }
                    ]
                }
            ])
        
        return blocks
    
    def _format_duration(self, seconds: float) -> str:
        """
        Formats duration in seconds to human readable format.
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    def send_error_alert(self, error_message: str, campaign_info: str = "") -> bool:
        """
        Sends a quick error alert to Slack.
        """
        if not self.client:
            return False
        
        try:
            text = f"🚨 *Scraping Error*\n{error_message}"
            if campaign_info:
                text += f"\n*Campaign:* {campaign_info}"
            
            self.client.chat_postMessage(
                channel=self.channel,
                text=text,
                unfurl_links=False
            )
            return True
            
        except Exception as e:
            logging.error(f"Failed to send error alert: {e}")
            return False


# Convenience function for quick usage
def send_scraping_summary(summary_data: Dict) -> bool:
    """
    Quick function to send scraping summary without instantiating class.
    """
    notifier = SlackNotifier()
    return notifier.send_scraping_summary(summary_data)
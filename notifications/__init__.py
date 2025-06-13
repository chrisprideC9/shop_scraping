# ── notifications/__init__.py ──

from .slack_notifier import SlackNotifier, send_scraping_summary

__all__ = ['SlackNotifier', 'send_scraping_summary']
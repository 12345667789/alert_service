# main.py - Simple Flask web server version
import os
import json
import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from flask import Flask, request, jsonify
from google.cloud import firestore
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Initialize Google Cloud clients
try:
    db = firestore.Client()
    logger.info("Firestore client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Firestore: {e}")
    db = None


@dataclass
class AlertData:
    """Core alert data structure"""

    alert_type: str  # 'new_breaker', 'ended_breaker', 're_breaker'
    symbol: str
    underlying: Optional[str] = None
    trigger_time: Optional[str] = None
    end_time: Optional[str] = None
    security_name: Optional[str] = None
    stock_price: Optional[float] = None
    analytics_summary: Optional[str] = None
    analytics_url: Optional[str] = None
    timestamp: Optional[str] = None
    source: str = "circuit_breaker_monitor"


class ConfigManager:
    """Manage configuration from Firestore"""

    def __init__(self):
        self._webhook_cache = {}
        self._cache_ttl = 300  # 5 minutes
        self._last_fetch = 0

    def get_webhook_config(self) -> dict:
        """Get webhook URLs from Firestore"""
        current_time = time.time()

        if current_time - self._last_fetch < self._cache_ttl and self._webhook_cache:
            return self._webhook_cache

        try:
            if not db:
                logger.error("Firestore client not available")
                return {}

            env = os.environ.get("ENVIRONMENT", "production")
            webhook_field_name = (
                "short_sale_alerts" if env == "production" else "short_sale_alerts_dev"
            )

            doc_ref = db.collection("app_config").document("discord_webhooks").get()
            if doc_ref.exists:
                data = doc_ref.to_dict()
                self._webhook_cache = {
                    "discord": data.get(webhook_field_name),
                    "telegram": data.get("telegram_bot_token"),
                }
                self._last_fetch = current_time
                logger.info(f"Loaded webhook config for environment: {env}")

        except Exception as e:
            logger.error(f"Failed to load webhook config: {e}")

        return self._webhook_cache


class AlertSender:
    """Handles alert delivery"""

    def __init__(self):
        self.config = ConfigManager()
        self.session = None

    async def get_session(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=10.0)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    def format_discord_alert(self, alert: AlertData) -> dict:
        """Format alert for Discord webhook"""
        colors = {
            "new_breaker": 0x00FF00,  # Green
            "ended_breaker": 0xFF0000,  # Red
            "re_breaker": 0xFFA500,  # Orange
        }

        emojis = {"new_breaker": "🟢", "ended_breaker": "🔴", "re_breaker": "🟠"}

        emoji = emojis.get(alert.alert_type, "⚪")
        title_map = {
            "new_breaker": f"{emoji} New Breaker: {alert.symbol}",
            "ended_breaker": f"{emoji} Breaker Ended: {alert.symbol}",
            "re_breaker": f"{emoji} Re-Breaker: {alert.symbol}",
        }

        title = title_map.get(alert.alert_type, f"{emoji} Alert: {alert.symbol}")

        description_parts = []
        if alert.underlying:
            description_parts.append(f"**Underlying:** {alert.underlying}")
        if alert.trigger_time:
            description_parts.append(f"**Trigger Time:** {alert.trigger_time} ET")
        if alert.end_time:
            description_parts.append(f"**End Time:** {alert.end_time} ET")
        if alert.stock_price:
            description_parts.append(f"**Current Price:** ${alert.stock_price:.2f}")

        description = "\n".join(description_parts)

        embed = {
            "title": title,
            "description": description,
            "color": colors.get(alert.alert_type, 0x808080),
            "timestamp": alert.timestamp,
            "footer": {"text": "Secret Alerts"},
        }

        return {"embeds": [embed]}

    async def send_discord_alert(self, alert: AlertData) -> bool:
        """Send alert to Discord webhook"""
        try:
            webhook_url = self.config.get_webhook_config().get("discord")
            if not webhook_url:
                logger.error("Discord webhook URL not configured")
                return False

            payload = self.format_discord_alert(alert)
            session = await self.get_session()

            async with session.post(webhook_url, json=payload) as response:
                if response.status == 204:
                    logger.info(f"Discord alert sent successfully for {alert.symbol}")
                    return True
                else:
                    logger.error(f"Discord alert failed: {response.status}")
                    return False

        except Exception as e:
            logger.error(f"Failed to send Discord alert for {alert.symbol}: {e}")
            return False

    async def send_alert(self, alert: AlertData) -> dict:
        """Send alert to configured channels"""
        results = {}
        results["discord"] = await self.send_discord_alert(alert)
        return results

    async def close(self):
        if self.session:
            await self.session.close()


# Initialize global services
alert_sender = AlertSender()


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    try:
        return (
            jsonify(
                {
                    "status": "healthy",
                    "timestamp": time.time(),
                    "service": "secret-alerts-dispatcher",
                }
            ),
            200,
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/", methods=["GET"])
def root():
    """Root endpoint"""
    return (
        jsonify(
            {
                "service": "Secret Alerts Dispatcher",
                "status": "running",
                "endpoints": {
                    "health": "/health",
                    "test_alert": "/test_alert (POST)",
                    "webhook": "/webhook (POST)",
                },
            }
        ),
        200,
    )


@app.route("/test_alert", methods=["POST"])
def test_alert():
    """Test alert endpoint"""
    try:
        data = request.get_json() or {}

        # Create test alert with defaults
        alert = AlertData(
            alert_type=data.get("alert_type", "new_breaker"),
            symbol=data.get("symbol", "TEST"),
            underlying=data.get("underlying", "TEST UNDERLYING"),
            trigger_time=data.get("trigger_time", "9:30:00"),
            timestamp=data.get("timestamp", time.time()),
        )

        # Process alert in background
        asyncio.create_task(process_alert_async(alert))

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Test alert queued for {alert.symbol}",
                    "alert_type": alert.alert_type,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Test alert failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """Webhook endpoint for receiving alerts"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided"}), 400

        # Create alert from webhook data
        alert = AlertData(
            alert_type=data.get("alert_type", "new_breaker"),
            symbol=data.get("symbol", "UNKNOWN"),
            underlying=data.get("underlying"),
            trigger_time=data.get("trigger_time"),
            end_time=data.get("end_time"),
            security_name=data.get("security_name"),
            timestamp=data.get("timestamp", time.time()),
        )

        # Process alert
        asyncio.create_task(process_alert_async(alert))

        return (
            jsonify(
                {"status": "success", "message": f"Alert queued for {alert.symbol}"}
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


async def process_alert_async(alert: AlertData):
    """Process alert asynchronously"""
    start_time = time.time()

    try:
        logger.info(f"Processing alert for {alert.symbol} - Type: {alert.alert_type}")

        # Send alert
        results = await alert_sender.send_alert(alert)

        total_time = time.time() - start_time
        logger.info(
            f"Alert processed for {alert.symbol} in {total_time:.2f}s - Results: {results}"
        )

    except Exception as e:
        total_time = time.time() - start_time
        logger.error(
            f"Alert processing failed for {alert.symbol} after {total_time:.2f}s: {e}"
        )


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"status": "error", "message": "Internal server error"}), 500


if __name__ == "__main__":
    # For local development
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
# Deployment test - Tue Sep  9 22:55:01 CDT 2025
# Auto-deploy test - Tue Sep  9 23:08:38 CDT 2025
# Fixed logging configuration - Tue Sep  9 23:19:32 CDT 2025
# Fixed logging configuration - Tue Sep  9 23:26:13 CDT 2025

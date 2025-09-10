# main.py - Minimal working version to fix memory issues
import os
import json
import time
import requests
from flask import Flask, request, jsonify
from google.cloud import firestore
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Initialize Firestore client (with error handling)
try:
    db = firestore.Client()
    logger.info("Firestore client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Firestore: {e}")
    db = None


class ConfigManager:
    """Simple config manager"""

    def __init__(self):
        self._webhook_cache = {}
        self._cache_ttl = 300
        self._last_fetch = 0

    def get_webhook_config(self) -> dict:
        """Get webhook URLs from Firestore with caching"""
        current_time = time.time()

        if current_time - self._last_fetch < self._cache_ttl and self._webhook_cache:
            return self._webhook_cache

        try:
            if not db:
                return {}

            env = os.environ.get("ENVIRONMENT", "production")
            webhook_field_name = (
                "short_sale_alerts" if env == "production" else "short_sale_alerts_dev"
            )

            doc_ref = db.collection("app_config").document("discord_webhooks").get()
            if doc_ref.exists:
                data = doc_ref.to_dict()
                self._webhook_cache = {"discord": data.get(webhook_field_name)}
                self._last_fetch = current_time
                logger.info(f"Loaded webhook config for environment: {env}")

        except Exception as e:
            logger.error(f"Failed to load webhook config: {e}")

        return self._webhook_cache


# Global config manager
config_manager = ConfigManager()


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "timestamp": time.time(),
            "service": "secret-alerts-dispatcher",
        }
    )


@app.route("/", methods=["GET"])
def root():
    """Root endpoint"""
    return jsonify(
        {
            "service": "Secret Alerts Dispatcher",
            "status": "running",
            "endpoints": {
                "health": "/health",
                "test_discord_sync": "/test_discord_sync (POST)",
                "webhook": "/webhook (POST)",
            },
        }
    )


@app.route("/test_discord_sync", methods=["POST"])
def test_discord_sync():
    """Simple synchronous Discord test"""
    try:
        data = request.get_json() or {}

        # Get Discord webhook
        webhook_url = config_manager.get_webhook_config().get("discord")
        if not webhook_url:
            return (
                jsonify(
                    {"status": "error", "message": "Discord webhook not configured"}
                ),
                400,
            )

        # Create test message
        symbol = data.get("symbol", "TEST")
        underlying = data.get("underlying", "Test Company")

        discord_payload = {
            "embeds": [
                {
                    "title": f"🟢 Test Alert: {symbol}",
                    "description": f"**Underlying:** {underlying}\n**Type:** test",
                    "color": 0x00FF00,
                    "footer": {"text": "Secret Alerts - Test"},
                }
            ]
        }

        # Send to Discord
        response = requests.post(webhook_url, json=discord_payload, timeout=10)

        if response.status_code == 204:
            return (
                jsonify(
                    {"status": "success", "message": f"Discord test sent for {symbol}"}
                ),
                200,
            )
        else:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Discord failed: {response.status_code}",
                    }
                ),
                500,
            )

    except Exception as e:
        logger.error(f"Discord test failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """Basic webhook endpoint"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided"}), 400

        symbol = data.get("symbol", "UNKNOWN")
        logger.info(f"Received webhook for {symbol}")

        return (
            jsonify({"status": "success", "message": f"Webhook received for {symbol}"}),
            200,
        )

    except Exception as e:
        logger.error(f"Webhook failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

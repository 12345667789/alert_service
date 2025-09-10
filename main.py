# main.py - Enhanced version with Telegram and environment selection
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

# Initialize Firestore client
try:
    db = firestore.Client()
    logger.info("Firestore client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Firestore: {e}")
    db = None


class ConfigManager:
    """Enhanced config manager with Telegram support"""

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

            # Get environment from Cloud Run env var or default to production
            env = os.environ.get("ENVIRONMENT", "production")

            # Select the right Discord webhook field
            discord_field = (
                "short_sale_alerts" if env == "production" else "short_sale_alerts_dev"
            )

            # Get Discord webhooks
            discord_doc = db.collection("app_config").document("discord_webhooks").get()
            discord_webhook = None
            if discord_doc.exists:
                discord_data = discord_doc.to_dict()
                discord_webhook = discord_data.get(discord_field)

            # Get Telegram config (assuming it's in a separate document)
            telegram_token = None
            telegram_chat_id = None
            try:
                telegram_doc = (
                    db.collection("app_config").document("telegram_config").get()
                )
                if telegram_doc.exists:
                    telegram_data = telegram_doc.to_dict()
                    telegram_token = telegram_data.get("bot_token")
                    telegram_chat_id = telegram_data.get("chat_id")
            except Exception as e:
                logger.warning(f"Could not load Telegram config: {e}")

            self._webhook_cache = {
                "discord": discord_webhook,
                "telegram_token": telegram_token,
                "telegram_chat_id": telegram_chat_id,
                "environment": env,
            }
            self._last_fetch = current_time
            logger.info(f"Loaded webhook config for environment: {env}")

        except Exception as e:
            logger.error(f"Failed to load webhook config: {e}")

        return self._webhook_cache


# Global config manager
config_manager = ConfigManager()


def send_telegram_message(token: str, chat_id: str, message: str) -> bool:
    """Send message to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}

        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Telegram message sent successfully")
            return True
        else:
            logger.error(f"Telegram failed: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


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
    config = config_manager.get_webhook_config()
    return jsonify(
        {
            "service": "Secret Alerts Dispatcher",
            "status": "running",
            "environment": config.get("environment", "unknown"),
            "endpoints": {
                "health": "/health",
                "test_discord_sync": "/test_discord_sync (POST)",
                "test_telegram": "/test_telegram (POST)",
                "test_both": "/test_both (POST)",
                "webhook": "/webhook (POST)",
            },
        }
    )


@app.route("/test_discord_sync", methods=["POST"])
def test_discord_sync():
    """Test Discord webhook"""
    try:
        data = request.get_json() or {}
        config = config_manager.get_webhook_config()

        webhook_url = config.get("discord")
        if not webhook_url:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Discord webhook not configured for {config.get('environment', 'unknown')} environment",
                    }
                ),
                400,
            )

        symbol = data.get("symbol", "TEST")
        underlying = data.get("underlying", "Test Company")

        discord_payload = {
            "embeds": [
                {
                    "title": f"🟢 Test Alert: {symbol}",
                    "description": f"**Underlying:** {underlying}\n**Environment:** {config.get('environment', 'unknown')}",
                    "color": 0x00FF00,
                    "footer": {"text": "Secret Alerts - Discord Test"},
                }
            ]
        }

        response = requests.post(webhook_url, json=discord_payload, timeout=10)

        if response.status_code == 204:
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": f"Discord test sent for {symbol} to {config.get('environment')} environment",
                    }
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


@app.route("/test_telegram", methods=["POST"])
def test_telegram():
    """Test Telegram webhook"""
    try:
        data = request.get_json() or {}
        config = config_manager.get_webhook_config()

        token = config.get("telegram_token")
        chat_id = config.get("telegram_chat_id")

        if not token or not chat_id:
            return (
                jsonify({"status": "error", "message": "Telegram not configured"}),
                400,
            )

        symbol = data.get("symbol", "TEST")
        underlying = data.get("underlying", "Test Company")

        message = f"🟢 *Test Alert: {symbol}*\n\n**Underlying:** {underlying}\n**Environment:** {config.get('environment', 'unknown')}\n\n_Secret Alerts - Telegram Test_"

        success = send_telegram_message(token, chat_id, message)

        if success:
            return (
                jsonify(
                    {"status": "success", "message": f"Telegram test sent for {symbol}"}
                ),
                200,
            )
        else:
            return jsonify({"status": "error", "message": "Telegram send failed"}), 500

    except Exception as e:
        logger.error(f"Telegram test failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/test_both", methods=["POST"])
def test_both():
    """Test both Discord and Telegram"""
    try:
        data = request.get_json() or {}
        results = {}

        # Test Discord
        try:
            discord_response = test_discord_sync()
            results["discord"] = "success" if discord_response[1] == 200 else "failed"
        except:
            results["discord"] = "failed"

        # Test Telegram
        try:
            telegram_response = test_telegram()
            results["telegram"] = "success" if telegram_response[1] == 200 else "failed"
        except:
            results["telegram"] = "failed"

        return jsonify({"status": "success", "results": results}), 200

    except Exception as e:
        logger.error(f"Test both failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """Basic webhook endpoint for receiving alerts"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided"}), 400

        symbol = data.get("symbol", "UNKNOWN")
        alert_type = data.get("alert_type", "unknown")

        logger.info(f"Received webhook for {symbol} - Type: {alert_type}")

        # Here you would process the alert and send to Discord/Telegram
        # For now, just acknowledge receipt

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Alert received for {symbol}",
                    "alert_type": alert_type,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Webhook failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

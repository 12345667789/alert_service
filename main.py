# main.py - Enhanced with flexible Pub/Sub integration and improved Telegram reliability
import os
import json
import time
import base64
import requests
from flask import Flask, request, jsonify
from google.cloud import firestore, pubsub_v1
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


class AlertProcessor:
    """Processes and enriches alert data"""

    @staticmethod
    def parse_minimal_alert(data: dict) -> dict:
        """Parse minimal alert data and standardize format"""
        # Handle different possible input formats
        symbol = data.get("symbol", "").upper()
        alert_type = data.get("alert_type", "new_breaker").lower().replace(" ", "_")
        trigger_time = data.get("trigger_time", "")
        underlying = data.get("underlying", symbol)  # Default to symbol if not provided

        # Handle timestamp - could be in different formats
        timestamp = data.get("timestamp")
        if not timestamp:
            timestamp = time.time()
        elif isinstance(timestamp, str):
            # If it's a string timestamp, try to convert or use current time
            try:
                timestamp = float(timestamp)
            except:
                timestamp = time.time()

        # Create standardized alert structure
        processed_alert = {
            "symbol": symbol,
            "alert_type": alert_type,
            "trigger_time": trigger_time,
            "underlying": underlying,
            "timestamp": timestamp,
            "source": data.get("source", "monitor_program"),
            # Future enrichment fields (empty for now)
            "stock_price": data.get("stock_price"),
            "volume": data.get("volume"),
            "market_cap": data.get("market_cap"),
            "sector": data.get("sector"),
        }

        logger.info(f"Processed alert: {symbol} - {alert_type} at {trigger_time}")
        return processed_alert

    @staticmethod
    def enrich_alert_data(alert: dict) -> dict:
        """Future function to enrich alert with additional data"""
        # This is where you could add:
        # - Stock price lookup
        # - Company information
        # - Market data
        # - Technical indicators
        # For now, just return as-is
        return alert


class NotificationSender:
    """Handles sending notifications to different channels with improved reliability"""

    def __init__(self, config_manager):
        self.config = config_manager

    def send_telegram_with_retry(self, message: str, max_retries: int = 2, delay: float = 1.0) -> str:
        """Send Telegram message with retry logic"""
        config = self.config.get_webhook_config()
        token = config.get("telegram_token")
        chat_id = config.get("telegram_chat_id")
        
        if not token or not chat_id:
            logger.warning("Telegram not configured - skipping")
            return "not_configured"
        
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }
        
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Sending Telegram message (attempt {attempt + 1})")
                response = requests.post(url, json=payload, timeout=10)
                
                if response.status_code == 200:
                    logger.info("Telegram message sent successfully")
                    return "success"
                elif response.status_code == 429:
                    # Rate limited - extract retry-after if available
                    retry_after = response.json().get("parameters", {}).get("retry_after", delay * 2)
                    logger.warning(f"Telegram rate limited. Retry after {retry_after} seconds")
                    if attempt < max_retries:
                        time.sleep(retry_after)
                        continue
                else:
                    logger.error(f"Telegram failed with status {response.status_code}: {response.text}")
                    if attempt < max_retries:
                        time.sleep(delay * (attempt + 1))  # Exponential backoff
                        continue
                        
            except requests.exceptions.Timeout:
                logger.error(f"Telegram timeout on attempt {attempt + 1}")
                if attempt < max_retries:
                    time.sleep(delay * (attempt + 1))
                    continue
            except Exception as e:
                logger.error(f"Telegram error on attempt {attempt + 1}: {e}")
                if attempt < max_retries:
                    time.sleep(delay * (attempt + 1))
                    continue
        
        logger.error("Telegram failed after all retry attempts")
        return "failed_after_retries"

    def format_discord_message(self, alert: dict) -> dict:
        """Format alert for Discord"""
        # Map alert types to colors and emojis
        type_config = {
            "new_breaker": {"emoji": "🟢", "color": 0x00FF00, "title": "New Breaker"},
            "ended_breaker": {
                "emoji": "🔴",
                "color": 0xFF0000,
                "title": "Breaker Ended",
            },
            "re_breaker": {"emoji": "🟡", "color": 0xFFA500, "title": "Re-Breaker"},
        }

        config = type_config.get(alert["alert_type"], type_config["new_breaker"])

        # Build description
        description_parts = [f"**Underlying:** {alert['underlying']}"]
        if alert["trigger_time"]:
            description_parts.append(f"**Trigger Time:** {alert['trigger_time']} ET")
        if alert.get("stock_price"):
            description_parts.append(f"**Price:** ${alert['stock_price']:.2f}")

        return {
            "embeds": [
                {
                    "title": f"{config['emoji']} {config['title']}: {alert['symbol']}",
                    "description": "\n".join(description_parts),
                    "color": config["color"],
                    "footer": {
                        "text": f"Secret Alerts | {alert.get('source', 'Monitor')} | Dispatcher Service"
                    },
                }
            ]
        }

    def format_telegram_message(self, alert: dict) -> str:
        """Format alert for Telegram"""
        emoji_map = {"new_breaker": "🟢", "ended_breaker": "🔴", "re_breaker": "🟡"}

        emoji = emoji_map.get(alert["alert_type"], "🟢")
        title = alert["alert_type"].replace("_", " ").title()

        message_parts = [
            f"{emoji} *{title}: {alert['symbol']}*",
            f"**Underlying:** {alert['underlying']}",
        ]

        if alert["trigger_time"]:
            message_parts.append(f"**Trigger Time:** {alert['trigger_time']} ET")
        if alert.get("stock_price"):
            message_parts.append(f"**Price:** ${alert['stock_price']:.2f}")

        message_parts.append(f"\n_Secret Alerts | {alert.get('source', 'Monitor')} | Pub/Sub System_")
        return "\n".join(message_parts)

    def send_notifications(self, alert: dict) -> dict:
        """Send alert to all configured channels with improved error handling"""
        results = {}
        config = self.config.get_webhook_config()

        # Send Discord first (usually more reliable)
        discord_webhook = config.get("discord")
        if discord_webhook:
            try:
                payload = self.format_discord_message(alert)
                logger.info("Sending Discord notification")
                response = requests.post(discord_webhook, json=payload, timeout=10)
                results["discord"] = (
                    "success"
                    if response.status_code == 204
                    else f"failed_{response.status_code}"
                )
                logger.info(f"Discord result: {results['discord']}")
            except Exception as e:
                logger.error(f"Discord send failed: {e}")
                results["discord"] = "error"

        # Add a short delay before sending to Telegram to avoid potential rate limiting
        if discord_webhook:
            time.sleep(0.5)

        # Send Telegram with retry logic
        telegram_message = self.format_telegram_message(alert)
        results["telegram"] = self.send_telegram_with_retry(telegram_message)

        # Log final results
        logger.info(f"Notification results: {results}")
        return results


class ConfigManager:
    """Enhanced config manager"""

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
            discord_field = (
                "short_sale_alerts" if env == "production" else "short_sale_alerts_dev"
            )

            # Get Discord webhooks
            discord_doc = db.collection("app_config").document("discord_webhooks").get()
            discord_webhook = None
            if discord_doc.exists:
                discord_data = discord_doc.to_dict()
                discord_webhook = discord_data.get(discord_field)

            # Get Telegram config
            telegram_token = None
            telegram_chat_id = None
            try:
                telegram_doc = (
                    db.collection("app_config").document("telegram_token").get()
                )
                if telegram_doc.exists:
                    telegram_data = telegram_doc.to_dict()
                    telegram_token = telegram_data.get("API_key")
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


# Initialize global components
config_manager = ConfigManager()
alert_processor = AlertProcessor()
notification_sender = NotificationSender(config_manager)


def publish_test_message(topic_name: str, alert_data: dict):
    """Publish a test message to Pub/Sub topic"""
    try:
        publisher = pubsub_v1.PublisherClient()
        project_id = "trading-analytics-2025"
        topic_path = publisher.topic_path(project_id, topic_name)

        message_data = json.dumps(alert_data).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()

        logger.info(f"Published message {message_id} to {topic_path}")
        return message_id

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        return None


# Routes
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
                "pubsub": "/pubsub (POST)",
                "test_pubsub": "/test_pubsub (POST)",
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

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
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
            results["discord"].append("failed")

        # Test Telegram
        try:
            telegram_response = test_telegram()
            results["telegram"] = "success" if telegram_response[1] == 200 else "failed"
        except:
            results["telegram"].append("failed")

        return jsonify({"status": "success", "results": results}), 200

    except Exception as e:
        logger.error(f"Test both failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    """Webhook endpoint for receiving alerts from monitoring program"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided"}), 400

        # Process the minimal alert data
        processed_alert = alert_processor.parse_minimal_alert(data)

        # Enrich with additional data (future enhancement)
        enriched_alert = alert_processor.enrich_alert_data(processed_alert)

        # Send notifications
        results = notification_sender.send_notifications(enriched_alert)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Alert processed for {processed_alert['symbol']}",
                    "results": results,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Webhook failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/pubsub", methods=["POST"])
def pubsub_handler():
    """Handle Pub/Sub messages from monitoring program"""
    request_start_time = time.time()
    logger.info("Pub/Sub handler started.")
    
    try:
        envelope = request.get_json()
        if not envelope:
            logger.warning("Request received with no JSON envelope.")
            return jsonify({"status": "error", "message": "No Pub/Sub message"}), 400

        pubsub_message = envelope.get("message")
        if not pubsub_message:
            logger.warning("Pub/Sub envelope received with no 'message' key.")
            return (
                jsonify({"status": "error", "message": "No message in envelope"}),
                400,
            )

        # Decode the message data
        message_data = pubsub_message.get("data")
        if message_data:
            logger.info("Decoding message data...")
            decoded_data = base64.b64decode(message_data).decode("utf-8")
            alert_data = json.loads(decoded_data)
            logger.info(f"Message decoded successfully: {alert_data.get('symbol')}")

            # Process the alert data
            logger.info("Processing alert...")
            processed_alert = alert_processor.parse_minimal_alert(alert_data)
            enriched_alert = alert_processor.enrich_alert_data(processed_alert)
            logger.info("Alert processing complete.")

            # Send notifications
            logger.info("Attempting to send notifications...")
            results = notification_sender.send_notifications(enriched_alert)
            logger.info("Notification sending complete.")

            request_duration = time.time() - request_start_time
            logger.info(f"Pub/Sub handler finished successfully in {request_duration:.2f}s.")
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": f"Pub/Sub alert processed for {processed_alert['symbol']}",
                        "results": results,
                    }
                ),
                200,
            )
        else:
            logger.warning("Pub/Sub message received with no 'data' key.")
            return jsonify({"status": "error", "message": "No data in message"}), 400

    except Exception as e:
        request_duration = time.time() - request_start_time
        logger.error(f"Pub/Sub handler error after {request_duration:.2f}s: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/test_pubsub", methods=["POST"])
def test_pubsub():
    """Test Pub/Sub publishing with minimal data structure"""
    try:
        data = request.get_json() or {}

        # Create minimal alert data matching your current format
        alert_data = {
            "symbol": data.get("symbol", "TEST"),
            "alert_type": data.get("alert_type", "new_breaker"),
            "trigger_time": data.get("trigger_time", "10:00:00"),
            "underlying": data.get("underlying", data.get("symbol", "TEST")),
            "timestamp": time.time(),
            "source": "test_pubsub",
        }

        topic_name = data.get("topic", "circuit-breaker-alerts")
        message_id = publish_test_message(topic_name, alert_data)

        if message_id:
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": f"Published test alert to topic {topic_name}",
                        "message_id": message_id,
                        "alert_data": alert_data,
                    }
                ),
                200,
            )
        else:
            return jsonify({"status": "error", "message": "Failed to publish"}), 500

    except Exception as e:
        logger.error(f"Test Pub/Sub failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
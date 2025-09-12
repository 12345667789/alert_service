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
        end_time = data.get("end_time", "") # Handle end_time as well
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
            "end_time": end_time,
            "underlying": underlying,
            "timestamp": timestamp,
            "source": data.get("source", "monitor_program"),
            # Future enrichment fields (empty for now)
            "stock_price": data.get("stock_price"),
            "volume": data.get("volume"),
            "market_cap": data.get("market_cap"),
            "sector": data.get("sector"),
        }

        logger.info(f"Processed alert: {symbol} - {alert_type}")
        return processed_alert

    @staticmethod
    def enrich_alert_data(alert: dict) -> dict:
        """Future function to enrich alert with additional data"""
        return alert


class NotificationSender:
    """Handles sending notifications to different channels with improved reliability"""

    def __init__(self, config_manager):
        self.config = config_manager
        
    def _escape_markdown(self, text: str) -> str:
        """Escape characters that are special in Telegram's MarkdownV2."""
        if not isinstance(text, str):
            text = str(text)
        
        # Characters to escape for MarkdownV2
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        
        # For each char in escape_chars, map it to '\\' + char
        translator = str.maketrans({char: f'\\{char}' for char in escape_chars})
        
        return text.translate(translator)

    def send_telegram_with_retry(self, message: str, max_retries: int = 2, delay: float = 1.0) -> str:
        """Send Telegram message with retry logic using MarkdownV2"""
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
            "parse_mode": "MarkdownV2",
        }
        
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Sending Telegram message (attempt {attempt + 1})")
                response = requests.post(url, json=payload, timeout=10)
                
                if response.status_code == 200:
                    logger.info("Telegram message sent successfully")
                    return "success"
                elif response.status_code == 429:
                    retry_after = response.json().get("parameters", {}).get("retry_after", delay * 2)
                    logger.warning(f"Telegram rate limited. Retry after {retry_after} seconds")
                    if attempt < max_retries:
                        time.sleep(retry_after)
                        continue
                else:
                    logger.error(f"Telegram failed with status {response.status_code}: {response.text}")
                    if attempt < max_retries:
                        time.sleep(delay * (attempt + 1))
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
        type_config = {
            "new_breaker": {"emoji": "🟢", "color": 0x00FF00, "title": "New Breaker"},
            "ended_breaker": {"emoji": "🔴", "color": 0xFF0000, "title": "Breaker Ended"},
            "re_breaker": {"emoji": "🟡", "color": 0xFFA500, "title": "Re-Breaker"},
        }

        config = type_config.get(alert["alert_type"], type_config["new_breaker"])

        description_parts = [f"**Underlying:** {alert['underlying']}"]
        if alert["trigger_time"]:
            description_parts.append(f"**Trigger Time:** {alert['trigger_time']} ET")
        if alert.get("end_time"):
            description_parts.append(f"**End Time:** {alert.get('end_time')} ET")
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
        """Format alert for Telegram with proper MarkdownV2 escaping"""
        emoji_map = {"new_breaker": "🟢", "ended_breaker": "🔴", "re_breaker": "🟡"}
        emoji = emoji_map.get(alert["alert_type"], "🟢")
        
        # Escape all dynamic parts of the message
        title = self._escape_markdown(alert["alert_type"].replace("_", " ").title())
        symbol = self._escape_markdown(alert['symbol'])
        underlying = self._escape_markdown(alert['underlying'])
        source = self._escape_markdown(alert.get('source', 'Monitor'))
        
        header = f"{emoji} *{title}: {symbol}*"
        
        message_parts = [
            header,
            f"*Underlying:* {underlying}",
        ]

        if alert["trigger_time"]:
            trigger_time = self._escape_markdown(alert['trigger_time'])
            message_parts.append(f"*Trigger Time:* {trigger_time} ET")
        if alert.get("end_time"):
            end_time = self._escape_markdown(alert.get('end_time'))
            message_parts.append(f"*End Time:* {end_time} ET")
        if alert.get("stock_price"):
            stock_price = self._escape_markdown(f"${alert['stock_price']:.2f}")
            message_parts.append(f"*Price:* {stock_price}")

        footer = f"\n_Secret Alerts | {source} | Dispatcher Service_"
        message_parts.append(footer)
        
        return "\n".join(message_parts)

    def send_notifications(self, alert: dict) -> dict:
        """Send alert to all configured channels with improved error handling"""
        results = {}
        config = self.config.get_webhook_config()

        discord_webhook = config.get("discord")
        if discord_webhook:
            try:
                payload = self.format_discord_message(alert)
                logger.info("Sending Discord notification")
                response = requests.post(discord_webhook, json=payload, timeout=10)
                results["discord"] = ("success" if response.status_code == 204 else f"failed_{response.status_code}")
                logger.info(f"Discord result: {results['discord']}")
            except Exception as e:
                logger.error(f"Discord send failed: {e}")
                results["discord"] = "error"

        if discord_webhook:
            time.sleep(0.5)

        telegram_message = self.format_telegram_message(alert)
        results["telegram"] = self.send_telegram_with_retry(telegram_message)

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
            if not db: return {}
            env = os.environ.get("ENVIRONMENT", "production")
            discord_field = ("short_sale_alerts" if env == "production" else "short_sale_alerts_dev")
            discord_doc = db.collection("app_config").document("discord_webhooks").get()
            discord_webhook = discord_doc.to_dict().get(discord_field) if discord_doc.exists else None
            telegram_doc = db.collection("app_config").document("telegram_token").get()
            telegram_token = telegram_doc.to_dict().get("API_key") if telegram_doc.exists else None
            telegram_chat_id = telegram_doc.to_dict().get("chat_id") if telegram_doc.exists else None

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
        project_id = os.environ.get("GCP_PROJECT", "trading-analytics-2025")
        topic_path = publisher.topic_path(project_id, topic_name)
        message_data = json.dumps(alert_data).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        return future.result()
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        return None

# Routes
@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "secret-alerts-dispatcher"})

@app.route("/", methods=["GET"])
def root():
    """Root endpoint"""
    config = config_manager.get_webhook_config()
    return jsonify({
        "service": "Secret Alerts Dispatcher",
        "status": "running",
        "environment": config.get("environment", "unknown")
    })

@app.route("/webhook", methods=["POST"])
def webhook():
    """Webhook for receiving alerts"""
    try:
        data = request.get_json()
        if not data: return jsonify({"status": "error", "message": "No JSON"}), 400
        processed_alert = alert_processor.parse_minimal_alert(data)
        enriched_alert = alert_processor.enrich_alert_data(processed_alert)
        results = notification_sender.send_notifications(enriched_alert)
        return jsonify({"status": "success", "results": results}), 200
    except Exception as e:
        logger.error(f"Webhook failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/pubsub", methods=["POST"])
def pubsub_handler():
    """Handle Pub/Sub messages"""
    request_start_time = time.time()
    logger.info("Pub/Sub handler started.")
    try:
        envelope = request.get_json()
        if not envelope or "message" not in envelope:
            return jsonify({"status": "error", "message": "Invalid Pub/Sub message"}), 400

        message_data = envelope["message"].get("data")
        if not message_data:
            return jsonify({"status": "error", "message": "No data in message"}), 400

        decoded_data = base64.b64decode(message_data).decode("utf-8")
        alert_data = json.loads(decoded_data)
        logger.info(f"Message decoded successfully: {alert_data.get('symbol')}")

        processed_alert = alert_processor.parse_minimal_alert(alert_data)
        enriched_alert = alert_processor.enrich_alert_data(processed_alert)
        results = notification_sender.send_notifications(enriched_alert)
        
        duration = time.time() - request_start_time
        logger.info(f"Pub/Sub handler finished in {duration:.2f}s.")
        return jsonify({"status": "success", "results": results}), 200
    except Exception as e:
        duration = time.time() - request_start_time
        logger.error(f"Pub/Sub handler error after {duration:.2f}s: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/test_pubsub", methods=["POST"])
def test_pubsub():
    """Test Pub/Sub publishing"""
    try:
        data = request.get_json() or {}
        alert_data = {"symbol": data.get("symbol", "TEST"), "alert_type": "new_breaker", "trigger_time": "10:00:00"}
        topic_name = data.get("topic", "circuit-breaker-alerts")
        message_id = publish_test_message(topic_name, alert_data)
        if message_id:
            return jsonify({"status": "success", "message_id": message_id}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to publish"}), 500
    except Exception as e:
        logger.error(f"Test Pub/Sub failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
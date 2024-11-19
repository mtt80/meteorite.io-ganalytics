import os
import json
import requests
import flask
import schedule
import threading
import time
import logging
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta import RunReportRequest
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

# Basic route to avoid 404 error
@app.route('/')
def home():
    return "Service is up and running!"

# Load environment variables with logging
GA_PROPERTY_ID = os.getenv('GA_PROPERTY_ID')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
CREDENTIALS_JSON = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')

# Validate environment variables
if not GA_PROPERTY_ID:
    logger.error("GA_PROPERTY_ID environment variable is not set")
if not DISCORD_WEBHOOK_URL:
    logger.error("DISCORD_WEBHOOK_URL environment variable is not set")
if not CREDENTIALS_JSON:
    logger.error("GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set")

if not all([GA_PROPERTY_ID, DISCORD_WEBHOOK_URL, CREDENTIALS_JSON]):
    logger.error("Required environment variables are missing")
    exit(1)

# Load service account credentials
try:
    credentials_info = json.loads(CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    logger.info("Successfully loaded service account credentials")
except json.JSONDecodeError as e:
    logger.error(f"Failed to parse credentials JSON: {e}")
    exit(1)
except Exception as e:
    logger.error(f"Error loading service account credentials: {e}")
    exit(1)

def get_analytics_data():
    """Fetch analytics data using GA4 API."""
    try:
        logger.info("Attempting to fetch analytics data...")
        client = BetaAnalyticsDataClient(credentials=credentials)
        
        request = RunReportRequest(
            property=f"properties/{GA_PROPERTY_ID}",
            dimensions=[{"name": "country"}],
            metrics=[{"name": "activeUsers"}],
            date_ranges=[{"start_date": "7daysAgo", "end_date": "today"}]
        )
        
        logger.info("Sending request to GA4 API...")
        response = client.run_report(request)
        
        # Build the message for Discord
        message = "🌍 GA4 Analytics Report:\n"
        for row in response.rows:
            country = row.dimension_values[0].value
            users = row.metric_values[0].value
            message += f"{country}: {users} users\n"
        
        logger.info("Successfully fetched analytics data")
        return message
    except Exception as e:
        logger.error(f"Error fetching analytics data: {str(e)}")
        return f"Error fetching analytics data: {str(e)}"

def send_to_discord(message):
    """Send message to Discord webhook."""
    if not message:
        logger.warning("No message to send to Discord")
        return
        
    try:
        logger.info("Attempting to send message to Discord...")
        response = requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
        
        if response.status_code == 204:
            logger.info("Successfully sent message to Discord")
        else:
            logger.error(f"Failed to send message to Discord. Status code: {response.status_code}")
            logger.error(f"Response content: {response.text}")
    except Exception as e:
        logger.error(f"Error sending to Discord: {str(e)}")

def analytics_job():
    """Job to fetch analytics data and send it to Discord."""
    logger.info("Starting analytics job...")
    message = get_analytics_data()
    send_to_discord(message)
    logger.info("Completed analytics job")

@app.route('/trigger')
def manual_trigger():
    logger.info("Manual trigger endpoint called")
    analytics_job()
    return "Triggered! Analytics job has been completed.", 200

def run_scheduler():
    """Scheduler that runs the job immediately after deployment and then every 10 minutes."""
    logger.info("Starting scheduler...")
    # Run the analytics job immediately after deployment
    analytics_job()
    
    # Schedule to run the job every 10 minutes
    schedule.every(10).minutes.do(analytics_job)
    logger.info("Scheduled job to run every 10 minutes")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    # Start scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True  # Make thread daemon so it exits when main thread exits
    scheduler_thread.start()
    
    # Run Flask app on Render with explicit debug mode
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=True)

import os
import json
import requests
import flask
import schedule
import threading
import time
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta import RunReportRequest
from google.oauth2 import service_account

app = flask.Flask(__name__)

# Load environment variables
GA_PROPERTY_ID = os.getenv('GA_PROPERTY_ID')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

# Load service account credentials from environment variable
try:
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json:
        credentials_info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
    else:
        print("Error: GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable not found.")
        exit(1)
except Exception as e:
    print(f"Error loading service account credentials: {e}")
    exit(1)

# Validate environment variables
if not GA_PROPERTY_ID or not DISCORD_WEBHOOK_URL:
    print("Error: Environment variables not set correctly.")
    exit(1)

def get_analytics_data():
    """Fetch analytics data using GA4 API."""
    try:
        client = BetaAnalyticsDataClient(credentials=credentials)
        request = RunReportRequest(
            property=f"properties/{GA_PROPERTY_ID}",
            dimensions=[{"name": "country"}],
            metrics=[{"name": "activeUsers"}],
            date_ranges=[{"start_date": "7daysAgo", "end_date": "today"}]
        )
        response = client.run_report(request)

        # Build the message for Discord
        message = "🌍 GA4 Analytics Report:\n"
        for row in response.rows:
            country = row.dimension_values[0].value
            users = row.metric_values[0].value
            message += f"{country}: {users} users\n"
        
        return message
    except Exception as e:
        return f"Error fetching analytics data: {str(e)}"

def send_to_discord(message):
    """Send message to Discord webhook."""
    if not message:
        return
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
        if response.status_code == 204:
            print("Successfully sent message to Discord.")
        else:
            print(f"Failed to send message to Discord: {response.status_code}")
    except Exception as e:
        print(f"Error sending to Discord: {str(e)}")

def analytics_job():
    """Job to fetch analytics data and send it to Discord."""
    message = get_analytics_data()
    send_to_discord(message)

@app.route('/trigger')
def manual_trigger():
    analytics_job()
    return "Triggered!", 200

def run_scheduler():
    schedule.every(10).minutes.do(analytics_job)
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    # Start scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()

    # Run Flask app
    app.run(host='0.0.0.0', port=5000)

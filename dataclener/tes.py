import time
import random
import psycopg2
import requests
from datetime import datetime, timedelta
import asyncio

# ZeptoMail API Configuration
ZEPTO_API_URL = "https://api.zeptomail.com/v1.1/email/template"
ZEPTO_API_KEY = "wSsVR61y/hT0XfsonDWvc+ptmlVXUl71EU583lKi7X6oS/vG88c8xRebBwL0FPAWQGJrEDUR9ukrmhkE0DMNitt4zF5TXSiF9mqRe1U4J3x17qnvhDzIXmlakBWIJYsLxAhsn2lkE8sl+g=="  # Replace with your actual API key
MAIL_TEMPLATE_KEY = "2d6f.5658c42c5ffe8491.k1.8a1d8090-13ab-11f0-8e03-525400a229b1.19610377719"

# Database Configuration
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "192.168.1.194"
DB_PORT = "5432"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
# Function to Insert Missing Clients into Email Queue
def insert_missing_clients():

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO master.email_queue_annual (client_id, email_id, annual_income, status)
    SELECT cd.client_id, cd.email_id, cd.annual_income, 'PENDING'
    FROM master.client_data_annual cd
    WHERE cd.status = 'active'
    AND NOT EXISTS (
        SELECT 1 FROM master.email_queue_annual eq WHERE eq.client_id = cd.client_id
    )
    RETURNING client_id;
    """  # Insert only clients who are not in `email_queue` and return inserted rows

    cursor.execute(insert_query)
    inserted_rows = cursor.fetchall()  # Fetch inserted client IDs

    conn.commit()
    cursor.close()

    print(f"âœ… Added {len(inserted_rows)} missing clients to email queue.")  # Print how many rows were added


# Function to Fetch Pending Emails
def fetch_pending_emails():
    cursor = conn.cursor()

    query = """
    SELECT eq.client_id, eq.email_id, cd.client_name, cd.annual_income
    FROM master.email_queue_annual eq
    JOIN master.client_data_annual cd ON eq.client_id = cd.client_id
    WHERE eq.status = 'PENDING';
    """

    cursor.execute(query)
    emails = cursor.fetchall()

    cursor.close()

    print(f"ðŸŸ¡ Debug: Fetched {len(emails)} pending emails.")  # Debugging
    return emails


# Function to Send Email via ZeptoMail (Using Templates)
def send_email(client_id, client_name, client_income, to_email):
    headers = {
        "Authorization": f"Zoho-enczapikey {ZEPTO_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "mail_template_key": MAIL_TEMPLATE_KEY,
        "from": {"address": "no_reply_annocement@enrichmoney.in", "name": "Enrich Money - Compliance"},
        "to": [{"email_address": {"address": to_email, "name": client_name}}],
        "merge_info": {"client_name": client_name, "client_code": client_id, "client_income": client_income},
    }

    print(f"ðŸŸ¡ Sending email to {to_email} (Client ID: {client_id})... {payload}")  # Debugging
    response = requests.post(ZEPTO_API_URL, json=payload, headers=headers)
    print(f"ðŸŸ¢ API Response: {response.status_code}, {response.text}")  # Debugging

    return response


# Function to Update Email Queue in Database
def update_email_status(client_id, email_id, status, response_message):
    try:
        cursor = conn.cursor()

        now = datetime.now()

        # Update `email_queue` with status and API response
        update_queue_query = """
        UPDATE master.email_queue_annual
        SET status = %s, sent_at = %s, response_message = %s
        WHERE client_id = %s AND email_id = %s;
        """
        cursor.execute(update_queue_query, (status, now, response_message, client_id, email_id))

        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        time.sleep(60)

def asyncrones_mail(pending_emails):
    for client_id, email_id, client_name, client_income in pending_emails:
        response = send_email(client_id, client_name, client_income, email_id)
        # print(response.json())
        if response.status_code == 201:
            print(f"âœ… Email sent to {email_id} (Client ID: {client_id})")
            update_email_status(client_id, email_id, "SENT", "Email sent successfully")
        else:
            print(f"âŒ Failed to send email to {email_id} (Client ID: {client_id}): {response.text}")
            update_email_status(client_id, email_id, "FAILED", response.text)
        time.sleep(random.randint(3,13))
# Main Function to Process Pending Emails
def process_emails():
    insert_missing_clients()  # Ensure all active clients are in email_queue
    pending_emails = fetch_pending_emails()

    if not pending_emails:
        print("âœ… No pending emails.")
        return
    batch_start_time = datetime.now() + timedelta(hours=1)
    for i in range(0, len(pending_emails), 400):
        batch_data = pending_emails[i:i + 400]
        asyncrones_mail(batch_data)
        diff = int((batch_start_time - datetime.now()).total_seconds())
        # time.sleep(diff)
        batch_start_time = batch_start_time + timedelta(hours=1)


if __name__ == "__main__":
    process_emails()

import os
import random
import pandas as pd
import time
from datetime import datetime
from google.cloud import storage

# -------------------------
# Config
# -------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("RAW_BUCKET")
OUTPUT_PREFIX = "synthetic_data"  # Folder name in bucket
NUM_USERS = 100
NUM_TRANSACTIONS = 200
NUM_NOTIFICATIONS = 100
NUM_DEVICES = 100

# -------------------------
# Helper functions
# -------------------------

def random_unix_date_feb_2019():
    """Generate random UNIX timestamp for February 2019."""
    day = random.randint(1, 28)
    dt = datetime(2019, 2, day)
    return int(time.mktime(dt.timetuple()))

def upload_to_gcs(df, filename):
    """Upload a dataframe as CSV to GCS."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{OUTPUT_PREFIX}/{filename}")
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"✅ Uploaded {filename} to gs://{BUCKET_NAME}/{OUTPUT_PREFIX}/")

# -------------------------
# Synthetic Data Generators
# -------------------------

def generate_users():
    countries = ["AT","AU","BE","BG","CH","CY","CZ","DE","DK","EE","ES","FI","FR","GB","IT","NL","NO","PL","PT","SE"]
    plans = ["METAL", "METAL_FREE", "PREMIUM", "PREMIUM_FREE", "PREMIUM_OFFER", "STANDARD"]

    data = []
    for i in range(NUM_USERS):
        data.append({
            "user_id": 100000 + i,
            "birth_year": random.randint(1950, 2001),
            "country": random.choice(countries),
            "created_date": random_unix_date_feb_2019(),
            "user_settings_crypto_unlocked": random.choice([True, False]),
            "plan": random.choice(plans),
            "attributes_notifications_marketing_push": random.choice([True, False]),
            "attributes_notifications_marketing_email": random.choice([True, False]),
            "num_contacts": random.randint(0, 50)
        })
    return pd.DataFrame(data)

def generate_transactions(user_ids):
    types = ["REFUND", "TOPUP", "EXCHANGE", "TRANSFER", "CARD_PAYMENT", "CARD_REFUND", "ATM", "FEE", "CASHBACK", "TAX"]
    currencies = ["EUR","USD","GBP","CHF","AUD","CAD","JPY","INR"]
    states = ["COMPLETED", "CANCELLED", "DECLINED", "PENDING", "REVERTED", "FAILED"]
    countries = ["DEU","FRA","ESP","GBR","USA","CHE","ITA","POL","NLD","AUS","CAN","MEX","BRA","ZAF","SGP"]

    data = []
    for i in range(NUM_TRANSACTIONS):
        data.append({
            "transaction_id": 200000 + i,
            "transactions_type": random.choice(types),
            "transactions_currency": random.choice(currencies),
            "amount_usd": round(random.uniform(1, 500), 2),
            "transactions_state": random.choice(states),
            "ea_cardholderpresence": random.choice([True, False]),
            "ea_merchant_country": random.choice(countries),
            "direction": random.choice(["INBOUND", "OUTBOUND"]),
            "user_id": random.choice(user_ids),
            "created_date": random_unix_date_feb_2019()
        })
    return pd.DataFrame(data)

def generate_notifications(user_ids):
    reasons = ["METAL_RESERVE_PLAN", "REENGAGEMENT_ACTIVE_FUNDS", "BLACK_FRIDAY", "LOST_CARD_ORDER",
               "JOINING_ANNIVERSARY", "WELCOME_HOME", "NO_INITIAL_CARD_USE"]
    channels = ["SMS", "PUSH", "EMAIL"]
    status = ["SENT", "FAILED"]

    data = []
    for i in range(NUM_NOTIFICATIONS):
        data.append({
            "reason": random.choice(reasons),
            "channel": random.choice(channels),
            "status": random.choice(status),
            "user_id": random.choice(user_ids),
            "created_date": random_unix_date_feb_2019()
        })
    return pd.DataFrame(data)

def generate_devices(user_ids):
    devices = ["Apple", "Android", "Unknown"]
    data = []
    for i in range(NUM_DEVICES):
        data.append({
            "device_type": random.choice(devices),
            "user_id": random.choice(user_ids)
        })
    return pd.DataFrame(data)

# -------------------------
# Main Execution
# -------------------------

def main():
    print("🚀 Generating synthetic data for Feb 2019...")

    users_df = generate_users()
    upload_to_gcs(users_df, "raw_users_2019_02.csv")

    transactions_df = generate_transactions(users_df["user_id"].tolist())
    upload_to_gcs(transactions_df, "raw_transactions_2019_02.csv")

    notifications_df = generate_notifications(users_df["user_id"].tolist())
    upload_to_gcs(notifications_df, "raw_notifications_2019_02.csv")

    devices_df = generate_devices(users_df["user_id"].tolist())
    upload_to_gcs(devices_df, "raw_devices_2019_02.csv")

    print("🎉 All synthetic CSV files uploaded successfully.")

if __name__ == "__main__":
    main()

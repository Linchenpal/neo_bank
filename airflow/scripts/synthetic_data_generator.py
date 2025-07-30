import pandas as pd
import random
import time
import os
from google.cloud import storage
from datetime import datetime, timedelta

# -----------------------
# CONFIG
# -----------------------
BUCKET_NAME = os.getenv("RAW_BUCKET", "neobank-raw-export")
OUTPUT_FILE = "synthetic_data/raw_users_2019_02.csv"

# Allowed values
COUNTRIES = ["AT","AU","BE","BG","CH","CY","CZ","DE","DK","EE","ES","FI","FR","GB","GF","GG","GI","GP",
             "GR","HR","HU","IE","IM","IS","IT","JE","LI","LT","LU","LV","MQ","MT","NL","NO","PL",
             "PT","RE","RO","SE","SI","SK"]

PLANS = ["METAL", "METAL_FREE", "PREMIUM", "PREMIUM_FREE", "PREMIUM_OFFER", "STANDARD"]

# -----------------------
# GENERATE SYNTHETIC DATA
# -----------------------
def generate_synthetic_users(num_records=1000):
    data = []

    # Define date range for February 2019
    start_date = datetime(2019, 2, 1)
    end_date = datetime(2019, 2, 28, 23, 59, 59)

    for i in range(1, num_records + 1):
        user_id = random.randint(10_000_000, 99_999_999)
        birth_year = random.randint(1950, 2003)
        country = random.choice(COUNTRIES)
        created_ts = int((start_date + (end_date - start_date) * random.random()).timestamp() * 1_000_000)  # microseconds
        crypto_unlocked = random.choice([True, False])
        plan = random.choice(PLANS)
        notif_push = random.choice([True, False])
        notif_email = random.choice([True, False])
        num_contacts = random.randint(0, 500)

        data.append([
            user_id, birth_year, country, created_ts,
            crypto_unlocked, plan, notif_push, notif_email, num_contacts
        ])

    df = pd.DataFrame(data, columns=[
        "user_id",
        "birth_year",
        "country",
        "created_date",
        "user_settings_crypto_unlocked",
        "plan",
        "attributes_notifications_marketing_push",
        "attributes_notifications_marketing_email",
        "num_contacts"
    ])

    return df

# -----------------------
# UPLOAD TO GCS
# -----------------------
def upload_to_gcs(local_file, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file)
    print(f"✅ Uploaded {local_file} to gs://{bucket_name}/{blob_name}")

# -----------------------
# MAIN SCRIPT
# -----------------------
def main():
    print("🔹 Generating synthetic raw_users data for Feb 2019...")
    df = generate_synthetic_users(1000)

    # Save locally
    local_path = "/tmp/raw_users_2019_02.csv"
    df.to_csv(local_path, index=False)

    # Upload to GCS
    upload_to_gcs(local_path, BUCKET_NAME, OUTPUT_FILE)

    print("✅ Synthetic data generation completed successfully!")

if __name__ == "__main__":
    main()

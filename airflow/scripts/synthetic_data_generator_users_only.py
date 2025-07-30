import pandas as pd
import numpy as np
import random
from datetime import datetime
from google.cloud import storage
import os

# -------------------------
# Config
# -------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("RAW_BUCKET")

# Fixed values based on your dataset
COUNTRIES = [
    "AT", "AU", "BE", "BG", "CH", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
    "FR", "GB", "GF", "GG", "GI", "GP", "GR", "HR", "HU", "IE", "IM", "IS",
    "IT", "JE", "LI", "LT", "LU", "LV", "MQ", "MT", "NL", "NO", "PL", "PT",
    "RE", "RO", "SE", "SI", "SK"
]

PLANS = ["STANDARD", "PREMIUM", "PREMIUM_FREE", "PREMIUM_OFFER", "METAL", "METAL_FREE"]

# -------------------------
# Functions
# -------------------------
def generate_users_data(year, month, num_records=500):
    """Generate synthetic data for raw_users table."""
    np.random.seed(year + month)  # deterministic results per month

    user_id = np.arange(1, num_records + 1) + int(f"{year}{month:02d}") * 1000
    birth_year = np.random.randint(1950, 2005, size=num_records)
    country = np.random.choice(COUNTRIES, size=num_records)
    created_date = int(datetime(year, month, 15).timestamp())  # middle of the month
    crypto_unlocked = np.random.choice([True, False], size=num_records, p=[0.3, 0.7])
    plan = np.random.choice(PLANS, size=num_records)
    notif_push = np.random.choice([True, False], size=num_records, p=[0.6, 0.4])
    notif_email = np.random.choice([True, False], size=num_records, p=[0.5, 0.5])
    num_contacts = np.random.randint(0, 50, size=num_records)

    df = pd.DataFrame({
        "user_id": user_id,
        "birth_year": birth_year,
        "country": country,
        "created_date": created_date,
        "user_settings_crypto_unlocked": crypto_unlocked,
        "plan": plan,
        "attributes_notifications_marketing_push": notif_push,
        "attributes_notifications_marketing_email": notif_email,
        "num_contacts": num_contacts
    })
    return df


def upload_to_gcs(bucket_name, blob_name, file_path):
    """Uploads a file to GCS."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"✅ Uploaded: {blob_name}")


def main():
    temp_file = "/tmp/raw_users.csv"

    # Loop from Feb 2019 to Dec 2020
    for year in [2019, 2020]:
        start_month = 2 if year == 2019 else 1
        for month in range(start_month, 13):
            # Generate data
            df = generate_users_data(year, month)

            # Save locally
            filename = f"raw_users_{year}_{month:02d}.csv"
            df.to_csv(temp_file, index=False)

            # Upload to GCS
            gcs_path = f"synthetic_data/{filename}"
            upload_to_gcs(BUCKET_NAME, gcs_path, temp_file)

    print("🎉 All synthetic data files generated and uploaded.")


if __name__ == "__main__":
    main()

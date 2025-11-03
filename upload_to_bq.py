from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
import time

# ✅ Configuration
project_id = "my-bq-project-12345"
dataset_id = "user_activity_data"
table_id = "activities"

# ✅ Path to your service account key file
key_path = "/mnt/c/Users/DELL/Downloads/my-bq-project-12345-aac195bdbbf3.json"

# ✅ Authenticate
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

try:
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # ✅ Generate sample data
    data = [
        {"user_id": 1, "activity": "login", "timestamp": str(datetime.now())},
        {"user_id": 2, "activity": "purchase", "timestamp": str(datetime.now())},
        {"user_id": 3, "activity": "logout", "timestamp": str(datetime.now())}
    ]
    df = pd.DataFrame(data)

    # ✅ Upload to BigQuery
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for upload to complete

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ Data uploaded successfully to BigQuery table: {table_ref} at {current_time}")

except Exception as e:
    print(f"❌ Error during upload: {e}")
    time.sleep(2)

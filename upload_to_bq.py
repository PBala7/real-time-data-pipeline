from google.cloud import bigquery
import pandas as pd

# Create a BigQuery client
client = bigquery.Client()

# Step 1: Replace this with your actual data (you can also load from a CSV)
data = [
    {"user_id": 1, "activity": "login", "timestamp": "2025-11-01 12:00:00"},
    {"user_id": 2, "activity": "purchase", "timestamp": "2025-11-01 12:05:00"},
    {"user_id": 3, "activity": "logout", "timestamp": "2025-11-01 12:10:00"}
]

# Convert to pandas DataFrame
df = pd.DataFrame(data)

# Step 2: Your BigQuery details
project_id = "my-bq-project-12345"
dataset_id = "user_activity_data"
table_id = "activities"

# Full table path
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Step 3: Upload DataFrame to BigQuery
job = client.load_table_from_dataframe(df, table_ref)
job.result()  # Waits for the upload to complete

print("✅ Data uploaded successfully to BigQuery table:", table_ref)

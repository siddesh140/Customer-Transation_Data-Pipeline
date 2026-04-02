import pandas as pd
import random
import os
from faker import Faker
import uuid

fake = Faker()

data = []

for _ in range(1000):
    transaction_id = str(uuid.uuid4())
    customer_id = f"C{random.randint(10000, 99999)}"

    # introduce bad data
    amount = random.choice(
        [round(random.uniform(10, 500), 2), 0, -random.uniform(1, 100)]
    )

    timestamp = fake.date_time_between(start_date="-2y", end_date="now")
    merchant = f"STORE_{random.randint(1,50)}"

    data.append([transaction_id, customer_id, amount, timestamp, merchant])

# Add duplicates
data.extend(data[:50])

df = pd.DataFrame(
    data, columns=["transaction_id", "customer_id", "amount", "timestamp", "merchant"]
)

# ✅ FIX STARTS HERE

# Use Airflow mounted path
output_dir = "/opt/airflow/dags/data"

# Create directory if not exists
os.makedirs(output_dir, exist_ok=True)

# Save file
output_path = os.path.join(output_dir, "transactions_raw.csv")
df.to_csv(output_path, index=False)

print(f"Data generated successfully at {output_path}!")

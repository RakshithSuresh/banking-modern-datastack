import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

bucket = os.getenv("S3_BUCKET_NAME")

def write_to_s3(table_name, records):
    if not records:
        return

    df = pd.DataFrame(records)

    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp_str = datetime.now().strftime("%H%M%S%f")

    local_file = f"{table_name}_{date_str}_{timestamp_str}.parquet"

    df.to_parquet(local_file, engine="fastparquet", index=False)

    s3_key = f"raw/{table_name}/date={date_str}/{table_name}_{timestamp_str}.parquet"

    s3.upload_file(local_file, bucket, s3_key)

    os.remove(local_file)

    print(f"✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}")

batch_size = 50

buffer = {
    "banking_server.public.customers": [],
    "banking_server.public.accounts": [],
    "banking_server.public.transactions": []
}

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    print(message.value)
    
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")

    if record:
        # record["_op"] = payload.get("op") #changes made by chatgpt
        # record["_event_ts_ms"] = payload.get("ts_ms") #changes made by chatgpt
        # record["_source_ts_ms"] = payload.get("source", {}).get("ts_ms") #changes made by chatgpt
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")

    if len(buffer[topic]) >= batch_size:
        write_to_s3(topic.split(".")[-1], buffer[topic])
        buffer[topic] = []
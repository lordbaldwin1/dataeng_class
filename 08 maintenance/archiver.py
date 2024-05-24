import pandas as pd
from google.cloud import pubsub_v1, storage
import json
from datetime import datetime
import traceback

project_id = "dataengproj-420923"
subscription_id = "archivetest-sub"
bucket_name = "archivebuckettesting"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

data_list = []

def callback(message):
    try:
        data = json.loads(message.data.decode('utf-8'))
        data_list.append(data)
        message.ack()
    except Exception as e:
        print("Error processing message:", str(e))
        traceback.print_exc()
        message.nack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Connected to {subscription_path}.")
try:
    streaming_pull_future.result(timeout=600)
except TimeoutError:
    print("Timeout of 10 minutes reached.")
except KeyboardInterrupt:
    print("Terminated by user.")
except Exception as e:
    print(f"An error occurred: {type(e).__name__}, {str(e)}")
    traceback.print_exc()
finally:
    streaming_pull_future.cancel()
    print("Stopped listening to messages.")

if data_list:
    data_frame = pd.DataFrame(data_list)

    current_date = datetime.now().strftime("%Y%m%d")
    csv_filename = f"raw_data_{current_date}.csv"
    json_filename = f"raw_data_{current_date}.json"

    csv_path = f"/tmp/{csv_filename}"
    data_frame.to_csv(csv_path, index=False)
    blob_csv = bucket.blob(csv_filename)
    blob_csv.upload_from_filename(csv_path)
    print(f"Uploaded {csv_filename} to {bucket_name}")

    json_path = f"/tmp/{json_filename}"
    data_frame.to_json(json_path, orient='records', lines=True)
    blob_json = bucket.blob(json_filename)
    blob_json.upload_from_filename(json_path)
    print(f"Uploaded {json_filename} to {bucket_name}")
else:
    print("No data to process.")
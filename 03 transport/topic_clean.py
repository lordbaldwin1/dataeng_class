from google.cloud import pubsub_v1

def discard_messages(project_id, subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        print(f"Discarded message ID: {message.message_id}")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}... Press Ctrl+C to exit.")

    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            try:
                streaming_pull_future.result()
            except Exception as e:
                print(f"Failed to cleanly shutdown: {e}")

if __name__ == "__main__":
    project_id = "dataengps1"
    subscription_id = "my-sub"
    discard_messages(project_id, subscription_id)
